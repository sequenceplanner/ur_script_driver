use r2r::{sensor_msgs, std_msgs, ur_script_msgs};
use r2r::{Context, Node, ParameterValue, Publisher, ServiceRequest};
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use futures::stream::{Stream, StreamExt};
use futures::future::{self, Either};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use ur_script_msgs::action::ExecuteScript;
use ur_script_msgs::srv::DashboardCommand as DBCommand;

struct DriverState {
    running: bool,
    connected: bool,
    // only handle one goal at the time. reply with true/false if
    // script executed successfully.
    goal_sender: Option<oneshot::Sender<bool>>,
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
}

impl DriverState {
    fn new() -> Self {
        DriverState {
            running: true,
            connected: false,
            goal_sender: None,
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
        }
    }
}

async fn handle_dashboard_commands(
    mut service: impl Stream<Item = ServiceRequest<DBCommand::Service>> + Unpin,
    dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
)  -> Result<(), std::io::Error> {
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

async fn action_server(
    ur_address: String,
    driver_state: Arc<Mutex<DriverState>>,
    dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
    mut requests: impl Stream<Item = r2r::ActionServerGoalRequest<ExecuteScript::Action>> + Unpin,
) -> Result<(), std::io::Error> {
    loop {
        match requests.next().await {
            Some(req) => {
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

                println!(
                    "Accepting goal request with goal id: {}, script '{}'",
                    req.uuid, req.goal.script
                );

                let (goal_sender, goal_receiver) = oneshot::channel::<bool>();
                let (mut g, mut cancel) = req.accept().expect("could not accept goal");

                println!("making a new connection to the driver.");
                let conn = TcpStream::connect(&ur_address).await;
                let mut write_stream = match conn {
                    Ok(write_stream) => write_stream,
                    Err(_) => {
                        println!("could not connect to realtime port for writing");
                        return Err(Error::new(ErrorKind::Other, "oh no!"));
                    }
                };
                println!("writing data to driver {}", g.goal.script);
                write_stream.write_all(g.goal.script.as_bytes()).await?;
                write_stream.flush().await?;

                {
                    let mut ds = driver_state.lock().unwrap();
                    ds.goal_sender = Some(goal_sender);
                }

                match future::select(goal_receiver, cancel.next()).await {
                    Either::Left((res, _cancel_stream)) => {
                        // success.
                        if let Ok(ok) = res {
                            println!("goal completed? {}", ok);
                            let result_msg = ExecuteScript::Result { ok };
                            // TODO: perhaps g.abort here if ok is false.
                            g.succeed(result_msg).expect("could not send result");
                        } else {
                            println!("future appears canceled...");
                            let result_msg = ExecuteScript::Result { ok: false };
                            g.abort(result_msg).expect("task cancelled");
                        }
                    },
                    Either::Right((request, goal_receiver)) => {
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

                            match future::select(cancel_receiver, goal_receiver).await {
                                Either::Left((res, _goal_receiver)) => {
                                    // cancelled using dashboard
                                    if let Ok(ok) = res {
                                        let result_msg = ExecuteScript::Result { ok };
                                        g.cancel(result_msg).expect("could not cancel goal");
                                    } else {
                                        println!("cancel dashboard future appears canceled");
                                        let result_msg = ExecuteScript::Result { ok: false };
                                        g.abort(result_msg).expect("could not cancel goal");
                                    }
                                },
                                Either::Right((res, _cancel_receiver)) => {
                                    // finished executing anyway
                                    if let Ok(ok) = res {
                                        let result_msg = ExecuteScript::Result { ok };
                                        g.succeed(result_msg).expect("could not succeed goal");
                                    } else {
                                        println!("finished executing but future appears canceled");
                                        let result_msg = ExecuteScript::Result { ok: false };
                                        g.abort(result_msg).expect("could not cancel goal");
                                    }
                                }
                            }
                        }
                    }
                };

                // at this point we have processed the goal.
                driver_state.lock().unwrap().goal_sender = None;
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
                println!("connected to: {}", address);
                return s;
            }
            Err(e) => {
                println!("could not connect to realtime at {}: {}", address, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn realtime_reader(
    driver_state: Arc<Mutex<DriverState>>,
    ur_address: String,
) -> Result<(), std::io::Error> {
    let mut checking_for_1 = false;
    let mut checking_for_2_since = None;
    let mut size_bytes = [0u8; 4];

    let mut stream = connect_loop(&ur_address).await;
    driver_state.lock().unwrap().connected = true;

    loop {
        let ret = timeout(
            Duration::from_millis(1000),
            stream.read_exact(&mut size_bytes),
        )
        .await;
        // handle outer timeout error
        if let Err(_) = ret {
            // reset state
            {
                let mut ds = driver_state.lock().unwrap();
                ds.goal_sender = None;
                ds.connected = false;
            }
            checking_for_1 = false;
            checking_for_2_since = None;

            println!("timeout on read, reconnecting... ");
            stream = connect_loop(&ur_address).await;
            driver_state.lock().unwrap().connected = true;

            continue;
        } else if let Ok(ret) = ret {
            if let Err(e) = ret {
                println!("unexpected read error: {}", e);
                return Err(Error::new(ErrorKind::Other, "oh no!"));
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
            let program_running = {
                let mut ds = driver_state.lock().unwrap();
                (*ds).joint_values = joints;
                (*ds).joint_speeds = speeds;
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
                (*ds).goal_sender.is_some()
            };

            let checking_for_2 = program_state == 1 && !checking_for_1;
            if program_running && checking_for_2 && checking_for_2_since.is_none() {
                // flank check for when we requested program (waiting for program state 2)
                checking_for_2_since = Some(std::time::Instant::now());
            } else if program_running && checking_for_2 && checking_for_2_since.is_some() {
                // we are currently waiting for program state == 2
                let elapsed_since_request = checking_for_2_since.unwrap().elapsed();
                if elapsed_since_request > std::time::Duration::from_millis(1000) {
                    // if there's been more than 1000 millis without the program
                    // entering the running state, abort this request.
                    checking_for_2_since = None;
                    {
                        let mut ds = driver_state.lock().unwrap();
                        if let Some(goal_sender) = ds.goal_sender.take() {
                            println!("program state never changed to running");
                            goal_sender.send(false).expect("goal receiver dropped");
                        }
                    }
                }
            }

            // when we have a goal, first wait until program_state reaches 2
            if program_running && program_state == 2 && !checking_for_1 {
                let elapsed = checking_for_2_since.map(|t|t.elapsed().as_millis()).unwrap_or_default();
                println!("program started after {}ms, waiting for finish", elapsed);
                checking_for_1 = true;
                checking_for_2_since = None;
            }

            // when the program state has been 2 and goes back to
            // 1, the goal has succeeded
            if checking_for_1 && program_state == 1 {
                println!("program started and has now finished");
                // reset state machine
                checking_for_1 = false;

                // we are finished. succeed and remove the action goal handle.
                {
                    let mut ds = driver_state.lock().unwrap();
                    if let Some(goal_sender) = ds.goal_sender.take() {
                        goal_sender.send(true).expect("goal receiver dropped");
                    } else {
                        println!("we fininshed but probably canceled the goal before...");
                    }
                }
            }

            if robot_state != 1 {
                // robot has entered protective or emergency stop. If
                // there is an active goal, abort it.  we are
                // finished. succeed and remove the action goal
                // handle.
                {
                    let mut ds = driver_state.lock().unwrap();
                    checking_for_1 = false;
                    checking_for_2_since = None;
                    if let Some(goal_sender) = ds.goal_sender.take() {
                        println!("aborting due to protective stop");
                        goal_sender.send(false).expect("goal receiver dropped");
                    }
                }
            }
        }
    }
}

async fn state_publisher(
    driver_state: Arc<Mutex<DriverState>>,
    joint_publisher: Publisher<sensor_msgs::msg::JointState>,
    measured_publisher: Publisher<ur_script_msgs::msg::Measured>,
) -> Result<(), std::io::Error> {
    let mut clock = r2r::Clock::create(r2r::ClockType::RosTime).unwrap();
    let joint_names = vec![
        format!("shoulder_pan_joint"),
        format!("shoulder_lift_joint"),
        format!("elbow_joint"),
        format!("wrist_1_joint"),
        format!("wrist_2_joint"),
        format!("wrist_3_joint"),
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
) -> Result<(), std::io::Error> {
    let stream = connect_loop(&ur_address).await;
    let mut stream = BufReader::new(stream);

    // eat welcome message
    let mut line = String::new();
    stream.read_line(&mut line).await?;
    if !line.contains("Connected: Universal Robots Dashboard Server") {
        return Err(Error::new(ErrorKind::Other, "oh no!"));
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

async fn flatten_error<T>(handle: JoinHandle<Result<T, Error>>) -> Result<T, Error> {
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
        .lock().unwrap().get("ur_address").as_ref()
    {
        s.to_owned()
    } else {
        // "192.168.2.125".to_owned()
        // "192.168.100.55".to_owned()
        "192.168.100.12".to_owned()
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
    let dashboard_service = node.create_service::<DBCommand::Service>("dashboard_command")?;

    let dashboard_task = handle_dashboard_commands(dashboard_service, txd);

    let shared_state = Arc::new(Mutex::new(DriverState::new()));

    let server_requests = node
        .create_action_server::<ExecuteScript::Action>("ur_script")?;

    let shared_state_action = shared_state.clone();
    let action_task = action_server(ur_address.to_owned(),
                                    shared_state_action,
                                    tx_dashboard,
                                    server_requests);

    let task_shared_state = shared_state.clone();

    let realtime_reader = realtime_reader(
        task_shared_state.clone(),
        ur_address.to_owned(),
    );
    let state_publisher =
        state_publisher(task_shared_state.clone(), joint_publisher, measured_publisher);

    let blocking_shared_state = task_shared_state.clone();
    let ros: JoinHandle<Result<(), Error>> = tokio::task::spawn_blocking(move || {
        while blocking_shared_state.lock().unwrap().running {
            node.spin_once(std::time::Duration::from_millis(8));
        }
        Ok(())
    });

    let dashboard = dashboard(rx_dashboard, ur_dashboard_address.to_owned());
    let ret = tokio::try_join!(
        action_task,
        realtime_reader,
        dashboard,
        state_publisher,
        dashboard_task,
        flatten_error(ros)
    );
    match ret {
        Err(e) => {
            (*task_shared_state.lock().unwrap()).running = false;
            return Err(Box::new(e));
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
