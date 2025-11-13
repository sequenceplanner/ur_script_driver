use r2r::{sensor_msgs, std_msgs, ur_script_msgs};
use r2r::{Context, Node, ParameterValue, Publisher, ServiceRequest, QosProfile};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::net::SocketAddr;
use futures::stream::{Stream, StreamExt};
use futures::future::{self, Either};
use futures::{SinkExt,FutureExt};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LinesCodec};
use tokio_util::task::LocalPoolHandle;
use ur_script_msgs::action::ExecuteScript;
use ur_script_msgs::srv::DashboardCommand as DBCommand;

const UR_DRIVER_SOCKET_PORT: u16 = 50000;

struct DriverState {
    running: bool,
    connected: bool,
    // only handle one goal at the time. reply with true/false if
    // script executed successfully.
    goal_id: Option<String>,
    goal_sender: Option<oneshot::Sender<bool>>,
    handshake_sender: Option<oneshot::Sender<bool>>,
    feedback_sender: Option<mpsc::Sender<String>>,
    robot_state: i32,
    program_state: i32,
    joint_values: Vec<f64>,
    joint_speeds: Vec<f64>,
    input_bit0: bool,
    input_bit1: bool,
    input_bit2: bool,
    input_bit3: bool,
    input_bit4: bool,
    input_bit5: bool,
    input_bit6: bool,
    input_bit7: bool,
    input_bit8: bool,
    input_bit9: bool,
    output_bit0: bool,
    output_bit1: bool,
    output_bit2: bool,
    output_bit3: bool,
    output_bit4: bool,
    output_bit5: bool,
    output_bit6: bool,
    output_bit7: bool,
    forces: Vec<f64>
}

impl DriverState {
    fn new() -> Self {
        DriverState {
            running: true,
            connected: false,
            goal_id: None,
            goal_sender: None,
            handshake_sender: None,
            feedback_sender: None,
            robot_state: 0,
            program_state: 0,
            joint_values: vec![],
            joint_speeds: vec![],
            input_bit0: false,
            input_bit1: false,
            input_bit2: false,
            input_bit3: false,
            input_bit4: false,
            input_bit5: false,
            input_bit6: false,
            input_bit7: false,
            input_bit8: false,
            input_bit9: false,
            output_bit0: false,
            output_bit1: false,
            output_bit2: false,
            output_bit3: false,
            output_bit4: false,
            output_bit5: false,
            output_bit6: false,
            output_bit7: false,
            forces: vec![]
        }
    }
}

async fn handle_dashboard_commands(
    mut service: impl Stream<Item = ServiceRequest<DBCommand::Service>> + Unpin,
    dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
)  -> Result<(), Box<dyn std::error::Error>> {
    loop {
        match service.next().await {
            Some(req) => {
                println!("got dashboard command request: {:?}", req.message);
                let dbc = if req.message.cmd.contains("reset_protective_stop") {
                    DashboardCommand::ResetProtectiveStop
                } else { // todo: implement more.
                    DashboardCommand::Stop
                };

                let (sender, future) = oneshot::channel();
                dashboard_commands.try_send((dbc, sender)).expect("could not send");
                let ret = future.await;
                let ok = ret.is_ok();

                let resp = DBCommand::Response { ok };
                req.respond(resp).expect("could not send service response");
            }
            None => break,
        }
    }
    Ok(())
}


/// Takes the original UR script to call and wraps it in handshaking
/// using socket communication to the host (caller).
fn generate_ur_script(original: &str, host_address: &str) -> String {
    let indented_script: String = original.lines().map(|l| format!("  {l}")).collect::<Vec<String>>().join("\n");

    let pre_script = r#"
def run_script():
"#.to_string();

    let post_script_1 = r#"

  def handshake():
"#.to_string();

    let post_script_2 = format!("    socket_open(\"{}\", 50000, \"ur_driver_socket\")", host_address);

    let post_script_3 = r#"
    line_from_server = socket_read_line("ur_driver_socket", timeout=1.0)
    if(str_empty(line_from_server)):
      return False
    else:
      socket_send_line(line_from_server, "ur_driver_socket")
      return True
    end
  end

  if(handshake()):
    result = script()
    if(result):
      socket_send_line("ok", "ur_driver_socket")
    else:
      socket_send_line("error", "ur_driver_socket")
    end
  else:
"#.to_string();
    let post_script_4 = format!("    popup(\"handshake failure with host {}, not moving.\")", host_address);

    let post_script_5 = r#"
  end

  socket_close("ur_driver_socket")
end

run_script()
"#.to_string();

    return format!("{}{}{}{}{}{}{}", pre_script, indented_script, post_script_1,
                   post_script_2, post_script_3, post_script_4, post_script_5);
}

async fn handle_request(
    ur_address: String,
    host_address: String,
    driver_state: Arc<Mutex<DriverState>>,
    dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
    mut g: r2r::ActionServerGoal<ExecuteScript::Action>,
    mut cancel: impl Stream<Item = r2r::ActionServerCancelRequest> + Unpin)
    -> Result<(), Box<dyn std::error::Error>> {
    let (goal_sender, goal_receiver) = oneshot::channel::<bool>();

    println!("making a new connection to the driver.");
    let conn = TcpStream::connect(&ur_address).await;
    let mut write_stream = match conn {
        Ok(write_stream) => write_stream,
        Err(_) => {
            println!("could not connect to realtime port for writing");
            return Err("oh no".into());
        }
    };

    // Append handshaking to the script.
    let script_to_write = generate_ur_script(&g.goal.script, &host_address);

    let (handshake_sender, handshake_receiver) = oneshot::channel::<bool>();
    let (feedback_sender, mut feedback_receiver) = tokio::sync::mpsc::channel(5);
    {
        let mut ds = driver_state.lock().unwrap();
        ds.goal_id = Some(g.uuid.to_string());
        ds.goal_sender = Some(goal_sender);
        ds.handshake_sender = Some(handshake_sender);
        ds.feedback_sender = Some(feedback_sender);
    }

    println!("writing data to driver\n{}", script_to_write);
    write_stream.write_all(script_to_write.as_bytes()).await?;
    write_stream.flush().await?;

    // check handshake first.
    enum ResultType { ABORTED, CANCELED, SUCCEDED }
    let (result_msg, result_type) = match timeout(Duration::from_millis(5000), handshake_receiver).await {
        Ok(Ok(true)) => {
            println!("HADNSHAKE OK, PERFORM NOMINAL");
            let publish_feedback_fut = async {
                loop {
                    match feedback_receiver.recv().await {
                        Some(msg) => {
                            let mut feedback_msg = ur_script_msgs::action::ExecuteScript::Feedback::default();
                            feedback_msg.feedback = msg;
                            g.publish_feedback(feedback_msg)?
                        },
                        None => {
                            return Result::<(), Box<dyn std::error::Error>>::Ok(())
                        },
                    }
                }
            };
            let nominal = Box::pin(async { futures::join!(goal_receiver, publish_feedback_fut) }).fuse();

            match future::select(nominal, cancel.next()).await {
                Either::Left(((res, _), _cancel_stream)) => {
                    // success.
                    if let Ok(ok) = res {
                        println!("goal completed. result: {}", ok);
                        let result_msg = ExecuteScript::Result { ok };
                        // TODO: perhaps g.abort here if ok is false.
                        (result_msg, ResultType::SUCCEDED)
                    } else {
                        println!("future appears canceled, abort.");
                        let result_msg = ExecuteScript::Result { ok: false };
                        (result_msg, ResultType::ABORTED)
                    }
                },
                Either::Right((request, nominal)) => {
                    if let Some(request) = request {
                        println!("got cancel request: {}", request.uuid);
                        request.accept();

                        // this starts a race between completing
                        // the motion and cancelling via stopping.
                        // goal removal is done by the one that
                        // succeeds first.
                        let (sender, cancel_receiver) = oneshot::channel();
                        dashboard_commands
                            .try_send((DashboardCommand::Stop, sender))
                            .expect("could not send...");

                        match future::select(cancel_receiver, nominal).await {
                            Either::Left((res, _nominal)) => {
                                // cancelled using dashboard
                                if let Ok(ok) = res {
                                    let result_msg = ExecuteScript::Result { ok };
                                    (result_msg, ResultType::CANCELED)
                                } else {
                                    println!("cancel dashboard future appears canceled");
                                    let result_msg = ExecuteScript::Result { ok: false };
                                    (result_msg, ResultType::ABORTED)
                                }
                            },
                            Either::Right(((res, _), _cancel_receiver)) => {
                                // finished executing before cancellation was complete
                                if let Ok(ok) = res {
                                    println!("goal completed. result: {}", ok);
                                    let result_msg = ExecuteScript::Result { ok };
                                    (result_msg, ResultType::SUCCEDED)
                                } else {
                                    println!("finished executing but future is canceled.");
                                    let result_msg = ExecuteScript::Result { ok: false };
                                    (result_msg, ResultType::ABORTED)
                                }
                            }
                        }
                    } else {
                        let result_msg = ExecuteScript::Result { ok: false };
                        println!("got cancel but sender seem dropped");
                        (result_msg, ResultType::ABORTED)
                    }
                }
            }
        },
        Ok(_) => {
            println!("HANDSHAKE FAILURE, ABORTING");
            let result_msg = ExecuteScript::Result { ok: false };
            (result_msg, ResultType::ABORTED)
        },
        Err(_) => {
            println!("HADNSHAKE TIMEOUT, ABORTING");
            let result_msg = ExecuteScript::Result { ok: false };
            (result_msg, ResultType::ABORTED)
        },
    };


    match result_type {
        ResultType::ABORTED => {
            g.abort(result_msg).expect("could not abort goal");
        },
        ResultType::CANCELED => {
            g.cancel(result_msg).expect("could not cancel goal");
        }
        ResultType::SUCCEDED => {
            g.succeed(result_msg).expect("could not succeed goal");
        }
    }

    // at this point we have processed this goal
    {
        let mut ds = driver_state.lock().unwrap();
        ds.goal_id = None;
        ds.goal_sender = None;
        ds.handshake_sender = None;
        ds.feedback_sender = None;
    }

    Ok(())
}

async fn action_server(
    ur_address: String,
    local_addr: watch::Receiver<Option<SocketAddr>>,
    driver_state: Arc<Mutex<DriverState>>,
    dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
    mut requests: impl Stream<Item = r2r::ActionServerGoalRequest<ExecuteScript::Action>> + Unpin,
) -> Result<(), Box<dyn std::error::Error>> {

    // we need this to spawn the goal onto a single thread since its not sync...
    let local_pool = LocalPoolHandle::new(1);

    loop {
        match requests.next().await {
            Some(req) => {
                let local_addr = local_addr.borrow().clone();
                if local_addr.is_none() {
                    println!(
                        "Have not connected to robot yet, rejecting request with goal id: {}, script: '{}'",
                        req.uuid, req.goal.script
                    );
                    req.reject().expect("could not reject goal");
                    continue;
                }
                // Local addr is not none, safe to unwrap here.
                let local_addr = local_addr.unwrap().ip().to_string();

                if !driver_state.lock().unwrap().connected {
                    println!(
                        "No connection to the robot, rejecting request with goal id: {}, script: '{}'",
                        req.uuid, req.goal.script
                    );
                    req.reject().expect("could not reject goal");
                    continue;
                }

                if driver_state.lock().unwrap().robot_state != 1 { //todo
                    println!(
                        "Robot is not in normal mode (state={}) stop, rejecting request with goal id: {}, script: '{}'",
                        driver_state.lock().unwrap().robot_state, req.uuid, req.goal.script
                    );
                    req.reject().expect("could not reject goal");
                    continue;
                }

                if let Some(goal_id) = driver_state.lock().unwrap().goal_id.as_ref().clone() {
                    println!(
                        "Already have a goal (id: {}), rejecting request with goal id: {}, script: '{}'",
                        goal_id, req.uuid, req.goal.script
                    );
                    req.reject().expect("could not reject goal");
                    continue;
                }

                println!(
                    "Accepting goal request with goal id: {}, script\n{}",
                    req.uuid, req.goal.script
                );

                let (g, cancel) = req.accept().expect("could not accept goal");

                let task_ur_address = ur_address.clone();
                let task_dashboard_commands = dashboard_commands.clone();
                let task_driver_state = driver_state.clone();
                local_pool.spawn_pinned(move || async {
                    let result = handle_request(task_ur_address,
                                                local_addr,
                                                task_driver_state,
                                                task_dashboard_commands,
                                                g,
                                                cancel,
                    ).await;
                    if let Err(e) = result {
                        println!("Error while handing goal: {}", e);
                    }
                });
            }
            None => break,
        }
    }
    Ok(())
}

fn read_f64(slice: &[u8]) -> f64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    f64::from_be_bytes(bytes)
}

async fn connect_loop(address: &str) -> TcpStream {
    loop {
        let ret = TcpStream::connect(address).await;
        match ret {
            Ok(s) => {
                let local_address = s.local_addr().expect("could net get local address");
                let peer_address = s.peer_addr().expect("could net get local address");
                println!("connected to: {} with host ip {}", peer_address, local_address);
                return s;
            }
            Err(e) => {
                println!("could not connect to realtime at {}: {}", address, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}


async fn socket_server(driver_state: Arc<Mutex<DriverState>>,
                       _dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
                       mut local_addr: watch::Receiver<Option<SocketAddr>>) -> Result<(), Box<dyn std::error::Error>> {
    let mut addr = None;
    while addr.is_none() {
        local_addr.changed().await?;
        addr = local_addr.borrow().clone();
    }

    let mut addr = addr.unwrap();
    addr.set_port(UR_DRIVER_SOCKET_PORT);

    println!("Starting socket server at {}", addr);

    let listener = TcpListener::bind(&addr).await?;
    loop {
        // Asynchronously wait for an inbound TcpStream.
        let (stream, addr) = listener.accept().await?;

        println!("New connection: {}", addr);

        // Only allow one active program at a time...
        // Check that the UR-script identifies with the correct goal id.
        // Otherwise we signal failure.

        let (goal_id,
             handshake_sender,
             feedback_sender) = {
            let mut ds = driver_state.lock().unwrap();

            if ds.handshake_sender.is_none() {
                println!("SHOULD NOT HAPPEN, DROPPING STREAM");
                continue;
            }
            let hss = ds.handshake_sender.take().unwrap();

            (ds.goal_id.clone(), hss, ds.feedback_sender.clone())
        };
        if goal_id.is_none() {
            println!("SHOULD NOT HAPPEN, DROPPING STREAM");
            continue;
        }
        if feedback_sender.is_none() {
            println!("SHOULD NOT HAPPEN, DROPPING STREAM");
            continue;
        }
        let goal_id = goal_id.unwrap();
        let feedback_sender = feedback_sender.unwrap();

        // perform initial handshake.
        let mut lines = Framed::new(stream, LinesCodec::new());

        // send goal id to ur script for handshaking.
        lines.send(&goal_id).await?;

        // read one line.
        let line = lines.next().await;
        match line {
            Some(Ok(s)) if s == goal_id => {
                println!("got GO with correct GOAL ID, start UR script.");
                if let Err(e) = handshake_sender.send(true) {
                    println!("could not send handshake result: {}", e);
                }
            }
            _ => {
                println!("got GO with incorrect GOAL ID, SHOULD NOT HAPPEN");
                if let Err(e) = handshake_sender.send(false) {
                    println!("could not send handshake result: {}", e);
                }
            }
        }

        let message = format!("Handshake complete, script should be running.");
        if let Err(e) = feedback_sender.send(message).await {
            println!("could not send feedback message: {}", e);
        }

        loop {
            match lines.next().await {
                Some(Ok(s)) if s == "ok".to_string() => {
                    println!("got OK, we are done.");

                    // we are finished. succeed and remove the action goal handle.
                    {
                        let mut ds = driver_state.lock().unwrap();
                        if let Some(goal_sender) = ds.goal_sender.take() {
                            goal_sender.send(true).expect("goal receiver dropped");
                        } else {
                            println!("we fininshed but probably canceled the goal before...");
                        }
                    }
                },
                Some(Ok(s)) if s == "error".to_string() => {
                    // we are finished. succeed and remove the action goal handle.
                    println!("got ERROR, we are done.");
                    {
                        let mut ds = driver_state.lock().unwrap();
                        if let Some(goal_sender) = ds.goal_sender.take() {
                            goal_sender.send(false).expect("goal receiver dropped");
                        } else {
                            println!("we fininshed but probably canceled the goal before...");
                        }
                    }
                },
                Some(Ok(s)) => {
                    // We got a line that is not ok or error,
                    // this should be send out as a feedback message.
                    println!("got {}, sending as feedback", s);
                    if let Err(e) = feedback_sender.send(s).await {
                        println!("could not send feedback message: {}", e);
                    }
                },
                _ => {
                    // TODO: handle failure here...
                    println!("Socket connection closed, dropping feedback sender.");
                    let mut ds = driver_state.lock().unwrap();
                    ds.feedback_sender = None;

                    break;
                }
            };
        }
    }
}

async fn realtime_reader(
    driver_state: Arc<Mutex<DriverState>>,
    ur_address: String,
    override_host_address: Option<String>,
    local_addr_sender: watch::Sender<Option<SocketAddr>>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut size_bytes = [0u8; 4];

    let mut stream = connect_loop(&ur_address).await;

    let local_addr = if let Some(s) = &override_host_address {
        SocketAddr::from_str(&format!("{}:0", s))?
    } else {
        stream.local_addr()?
    };
    local_addr_sender.send(Some(local_addr))?;

    driver_state.lock().unwrap().connected = true;

    loop {
        let ret = timeout(
            Duration::from_millis(1000),
            stream.read_exact(&mut size_bytes),
        )
        .await;
        // handle outer timeout error
        if let Err(_) = ret {
            {
                // We no longer kill the goal here since we know the state of execution
                // via the socket communication.
                let mut ds = driver_state.lock().unwrap();
                ds.connected = false;
            }
            println!("timeout on read, reconnecting... ");
            stream = connect_loop(&ur_address).await;
            driver_state.lock().unwrap().connected = true;

            continue;
        } else if let Ok(ret) = ret {
            if let Err(e) = ret {
                println!("unexpected read error: {}", e);
                return Err("oh no".into());
            }
        }
        let msg_size = u32::from_be_bytes(size_bytes) as usize;

        // need to subtract the 4 we already read (msg_size)
        let mut buf: Vec<u8> = Vec::new();
        buf.resize(msg_size - 4, 0);
        stream.read_exact(&mut buf).await?;

        if msg_size != 1220 {
            println!("got unkown frame length: {}", msg_size);
        }
        if msg_size == 1220 {
            // let time = read_f64(&buf[0..8]);
            let mut joints = vec![];
            let mut speeds = vec![];
            for i in 0..6 {
                let index = 248 + i * 8;
                let joint_val = read_f64(&buf[index..index + 8]);
                joints.push(joint_val);
            }

            for i in 0..6 {
                let index = 296 + i * 8;
                let joint_speed = read_f64(&buf[index..index + 8]);
                speeds.push(joint_speed);
            }

            let mut forces = vec![];
            for i in 0..6 {
                let index = 536 + i * 8;
                let f = read_f64(&buf[index..index + 8]);
                forces.push(f);
            }

            // println!("force vector {:?}", forces);

            let digital_inputs = read_f64(&buf[680..688]) as u32;

            let robot_state = read_f64(&buf[808..816]) as i32;
            // println!("robot state {:?}", robot_state);

            let digital_outputs = read_f64(&buf[1040..1048]) as u32;
            // println!("digital out state {:?}", digital_outputs);

            let program_state = read_f64(&buf[1048..1056]) as i32;
            // println!("program state {:?}", program_state);

            // update program state.
            {
                let mut ds = driver_state.lock().unwrap();
                (*ds).joint_values = joints;
                (*ds).joint_speeds = speeds;
                (*ds).forces = forces;
                (*ds).robot_state = robot_state.clone();
                (*ds).program_state = program_state.clone();
                (*ds).input_bit0 = digital_inputs & 1 == 1;
                (*ds).input_bit1 = digital_inputs & 2 == 2;
                (*ds).input_bit2 = digital_inputs & 4 == 4;
                (*ds).input_bit3 = digital_inputs & 8 == 8;
                (*ds).input_bit4 = digital_inputs & 16 == 16;
                (*ds).input_bit5 = digital_inputs & 32 == 32;
                (*ds).input_bit6 = digital_inputs & 64 == 64;
                (*ds).input_bit7 = digital_inputs & 128 == 128;
                (*ds).input_bit8 = digital_inputs & 65536 == 65536;
                (*ds).input_bit9 = digital_inputs & 131072 == 131072;

                (*ds).output_bit0 = digital_outputs & 1 == 1;
                (*ds).output_bit1 = digital_outputs & 2 == 2;
                (*ds).output_bit2 = digital_outputs & 4 == 4;
                (*ds).output_bit3 = digital_outputs & 8 == 8;
                (*ds).output_bit4 = digital_outputs & 16 == 16;
                (*ds).output_bit5 = digital_outputs & 32 == 32;
                (*ds).output_bit6 = digital_outputs & 64 == 64;
                (*ds).output_bit7 = digital_outputs & 128 == 128;
            };

            if robot_state != 1 {
                // robot has entered protective or emergency stop. If
                // there is an active goal, abort it.  we are
                // finished. succeed and remove the action goal
                // handle.
                {
                    let mut ds = driver_state.lock().unwrap();
                    if let Some(goal_sender) = ds.goal_sender.take() {
                        println!("aborting due to protective stop");
                        goal_sender.send(false).expect("goal receiver dropped");
                    }
                    ds.feedback_sender = None;
                    ds.goal_id = None;
                }
            }
        }
    }
}

async fn state_publisher(
    driver_state: Arc<Mutex<DriverState>>,
    joint_publisher: Publisher<sensor_msgs::msg::JointState>,
    measured_publisher: Publisher<ur_script_msgs::msg::Measured>,
    prefix: String
) -> Result<(), Box<dyn std::error::Error>> {
    let mut clock = r2r::Clock::create(r2r::ClockType::RosTime).unwrap();
    let joint_names = vec![
        format!("{}shoulder_pan_joint", prefix),
        format!("{}shoulder_lift_joint", prefix),
        format!("{}elbow_joint", prefix),
        format!("{}wrist_1_joint", prefix),
        format!("{}wrist_2_joint", prefix),
        format!("{}wrist_3_joint", prefix),
    ];

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let now = clock.get_now().unwrap();
        let time = r2r::Clock::to_builtin_time(&now);

        let (joint_values, measured) = {
            let ds = driver_state.lock().unwrap();

            let measured = ur_script_msgs::msg::Measured {
                robot_state: (*ds).robot_state,
                program_state: (*ds).program_state,
                forces: (*ds).forces.clone(),

                in0: (*ds).input_bit0,
                in1: (*ds).input_bit1,
                in2: (*ds).input_bit2,
                in3: (*ds).input_bit3,
                in4: (*ds).input_bit4,
                in5: (*ds).input_bit5,
                in6: (*ds).input_bit6,
                in7: (*ds).input_bit7,
                in8: (*ds).input_bit8,
                in9: (*ds).input_bit9,

                out0: (*ds).output_bit0,
                out1: (*ds).output_bit1,
                out2: (*ds).output_bit2,
                out3: (*ds).output_bit3,
                out4: (*ds).output_bit4,
                out5: (*ds).output_bit5,
                out6: (*ds).output_bit6,
                out7: (*ds).output_bit7,
            };
            ((*ds).joint_values.clone(), measured)
        };

        let header = std_msgs::msg::Header {
            stamp: time,
            ..Default::default()
        };
        let to_send = sensor_msgs::msg::JointState {
            header,
            position: joint_values,
            name: joint_names.clone(),
            ..Default::default()
        };

        joint_publisher.publish(&to_send).unwrap();
        measured_publisher.publish(&measured).unwrap();
    }
}

#[derive(Clone, PartialEq, Debug)]
enum DashboardCommand {
    Stop,
    ResetProtectiveStop,
}

async fn dashboard(
    mut recv: tokio::sync::mpsc::Receiver<(DashboardCommand, oneshot::Sender<bool>)>,
    ur_address: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let stream = connect_loop(&ur_address).await;
    let mut stream = BufReader::new(stream);

    // eat welcome message
    let mut line = String::new();
    stream.read_line(&mut line).await?;
    if !line.contains("Connected: Universal Robots Dashboard Server") {
        return Err("oh no".into());
    }

    stream
        .write_all(String::from("get robot model\n").as_bytes())
        .await?;
    stream.flush().await?;
    let mut robot_model = String::new();
    stream.read_line(&mut robot_model).await?;
    println!("robot model: {}", robot_model);

    // check that robot is in remote control
    // check commented out to work with simulator...
    // stream
    //     .write_all(String::from("is in remote control\n").as_bytes())
    //     .await?;
    // let mut line = String::new();
    // stream.read_line(&mut line).await?;
    // if !line.contains("true") {
    //     println!("remote mode reply: {}", line);
    //     return Err(Error::new(ErrorKind::Other, "must be in remote mode"));
    // }

    loop {
        let (cmd, channel) = recv.recv().await.unwrap();

        println!("dashboard writer got command {:?}", cmd);

        let (command, expected_response) = match cmd {
            DashboardCommand::Stop => ("stop\n", "Stopped"),
            DashboardCommand::ResetProtectiveStop => {
                ("unlock protective stop\n", "Protective stop releasing")
            }
        };

        println!("writing command to driver {}", command);
        stream.write_all(command.as_bytes()).await?;
        stream.flush().await?;

        let mut response = String::new();
        stream.read_line(&mut response).await?;

        if response.contains(expected_response) {
            if let Err(_) = channel.send(true) {
                println!("dropped dashboard return channel");
                // ignore this error for now?
            }
        } else {
            println!(
                "failed to execute command via dashboard: {}, expected: {}",
                response, expected_response
            );
            if let Err(_) = channel.send(false) {
                println!("dropped dashboard return channel");
                // ignore this error for now?
            }
        }
    }
}

async fn flatten_error<T>(handle: JoinHandle<Result<T, Box<dyn std::error::Error + Send>>>) -> Result<T, Box<dyn std::error::Error>> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(err) => Err(err.into()),
    }
}

///
/// ur driver
///
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let ros_ctx = Context::create()?;
    let mut node = Node::create(ros_ctx, "ur_script_driver", "")?;

    let ur_address = if let Some(ParameterValue::String(s)) = node.params
        .lock().unwrap().get("ur_address").map(|p| &p.value)
    {
        s.to_owned()
    } else {
        "localhost".to_owned()
    };

    let override_host_address: Option<String> = node.params.lock().unwrap()
        .get("override_host_address").and_then(|k| {
            if let ParameterValue::String(s) = &k.value {
                Some(s.clone())
            } else { None }
        });

    let prefix = if let Some(ParameterValue::String(s)) = node.params
        .lock().unwrap().get("prefix").map(|p| &p.value)
    {
        s.to_owned()
    } else {
        "".to_owned()
    };

    let ur_dashboard_address = format!("{}:29999", ur_address);
    let ur_address = format!("{}:30003", ur_address);

    let joint_publisher = node
        .create_publisher::<sensor_msgs::msg::JointState>("joint_states",
                                                          r2r::qos::QosProfile::default())?;
    let measured_publisher = node
        .create_publisher::<ur_script_msgs::msg::Measured>("measured",
                                                           r2r::qos::QosProfile::default())?;

    let (tx_dashboard, rx_dashboard) =
        mpsc::channel::<(DashboardCommand, oneshot::Sender<bool>)>(10);

    let txd = tx_dashboard.clone();
    let dashboard_service = node.create_service::<DBCommand::Service>("dashboard_command", QosProfile::default())?;

    let dashboard_task = handle_dashboard_commands(dashboard_service, txd);

    let shared_state = Arc::new(Mutex::new(DriverState::new()));

    let server_requests = node
        .create_action_server::<ExecuteScript::Action>("ur_script")?;

    let (local_addr_sender, local_addr_receiver) = watch::channel(None);

    let shared_state_action = shared_state.clone();
    let action_task = action_server(ur_address.to_owned(),
                                    local_addr_receiver.clone(),
                                    shared_state_action,
                                    tx_dashboard.clone(),
                                    server_requests);

    let task_shared_state = shared_state.clone();


    let realtime_reader = realtime_reader(
        task_shared_state.clone(),
        ur_address.to_owned(),
        override_host_address,
        local_addr_sender,
    );
    let state_publisher =
        state_publisher(task_shared_state.clone(), joint_publisher, measured_publisher, prefix);

    let blocking_shared_state = task_shared_state.clone();
    let ros: JoinHandle<Result<(), Box<dyn std::error::Error + Send>>> = tokio::task::spawn_blocking(move || {
        while blocking_shared_state.lock().unwrap().running {
            node.spin_once(std::time::Duration::from_millis(8));
        }
        Ok(())
    });

    let socket_server = socket_server(task_shared_state.clone(),
                                      tx_dashboard,
                                      local_addr_receiver);

    let dashboard = dashboard(rx_dashboard, ur_dashboard_address.to_owned());
    let ret = tokio::try_join!(
        action_task,
        realtime_reader,
        socket_server,
        dashboard,
        state_publisher,
        dashboard_task,
        flatten_error(ros)
    );
    match ret {
        Err(e) => {
            (*task_shared_state.lock().unwrap()).running = false;
            return Err(e.into());
        }
        Ok(_) => {
            // will never get here.
            return Ok(());
        }
    }
}

#[tokio::main]
async fn main() -> () {
    loop {
        let ret = run().await;
        match ret {
            Err(e) => {
                println!("fatal error: {}", e);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            _ => {}
        }
    }
}