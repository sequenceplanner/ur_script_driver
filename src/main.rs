use r2r::{sensor_msgs, std_msgs, ur_script_msgs};
use r2r::{Context, Node, ParameterValue, Publisher, ServerGoal};
use std::sync::{Arc, Mutex};
use std::io::{Error, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use ur_script_msgs::action::ExecuteScript;
use ur_script_msgs::srv::DashboardCommand as DBCommand;
use std::time::Duration;

#[derive(Clone)]
struct DriverState {
    // only handle one goal at the time.
    // later think about allowing goals to be queued up
    goal: Option<ServerGoal<ExecuteScript::Action>>,
    robot_state: String,
    program_state: String,
    joint_values: Vec<f64>,
    joint_speeds: Vec<f64>,
}

impl DriverState {
    fn new() -> Self {
        DriverState {
            goal: None,
            robot_state: "Idle".into(),
            program_state: "0".into(),
            joint_values: vec![],
            joint_speeds: vec![],
        }
    }
}

// note: cannot be blocking.
// todo: do more here such as checking if we are in protective stop
fn accept_goal_cb(
    driver_state: Arc<Mutex<DriverState>>,
    uuid: &r2r::uuid::Uuid,
    goal: &ExecuteScript::Goal,
) -> bool {
    if driver_state.lock().unwrap().goal.is_some() {
        println!(
            "Already have a goal, rejecting request with goal id: {}, script: '{}'",
            uuid, goal.script
        );
        return false;
    }
    println!(
        "Accepting goal request with goal id: {}, script '{}'",
        uuid, goal.script
    );
    true // always accept
}

// note: cannot be blocking.
fn accept_cancel_cb(goal: &r2r::ServerGoal<ExecuteScript::Action>) -> bool {
    println!("Got request to cancel {}", goal.uuid);
    // always accept cancel requests
    true
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
            },
            Err(e) => {
                println!("could not connect to realtime at {}: {}", address, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn realtime_writer(
    mut incoming_scripts: mpsc::Receiver<std::string::String>,
    ur_address: String) -> Result<(), std::io::Error> {
    loop {
        match incoming_scripts.recv().await {
            Some(data) => {
                println!("making a new connection to the driver.");
                let ret = TcpStream::connect(&ur_address).await;
                match ret {
                    Ok(mut write_stream) => {
                        println!("writing data to driver {}", data);
                        write_stream.write_all(data.as_bytes()).await?;
                        write_stream.flush().await?;
                    },
                    Err(_) => {
                        println!("could not connect to realtime port for writing");
                        return Err(Error::new(ErrorKind::Other, "oh no!"));
                    }
                }
            }
            None => {
                println!("channel closed");
                return Err(Error::new(ErrorKind::Other, "oh no!"));
            }
        }
    }
}

async fn realtime_reader(
    driver_state: Arc<Mutex<DriverState>>,
    dashboard_commands: mpsc::Sender<(DashboardCommand, oneshot::Sender<bool>)>,
    ur_address: String,
) -> Result<(), std::io::Error> {
    let mut checking_for_1 = false;
    let mut cancelling = false;
    let mut stream = connect_loop(&ur_address).await;
    let mut size_bytes = [0u8; 4];

    loop {
        let ret = timeout(Duration::from_millis(1000), stream.read_exact(&mut size_bytes)).await;
        // handle outer timeout error
        if let Err(_) = ret {
            println!("timeout on read, reconnecting...");
            stream = connect_loop(&ur_address).await;
            continue;
        } else if let Ok(ret) = ret {
            if let Err(e) = ret {
                println!("unexpected read error: {}", e);
                return Err(Error::new(ErrorKind::Other, "oh no!"))
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
            let robot_state = read_f64(&buf[808..816]).to_string();
            // println!("robot state {:?}", robot_state);
            let program_state = read_f64(&buf[1048..1056]).to_string();
            // println!("program state {:?}", program_state);

            // update program state.
            let program_running = {
                let mut ds = driver_state.lock().unwrap();
                (*ds).joint_values = joints;
                (*ds).joint_speeds = speeds;
                (*ds).robot_state = robot_state.clone();
                (*ds).program_state = program_state.clone();
                (*ds).goal.is_some()
            };

            // when we have a goal, first wait until program_state reaches "2'
            if program_running && program_state == "2" && !checking_for_1 {
                println!("program started, waiting for finish");
                checking_for_1 = true;
            }

            // when the program state has been "2" and goes back to
            // "1", the goal has succeeded
            if checking_for_1 && program_state == "1" {
                println!("program started and has now finished");
                // reset state machine
                checking_for_1 = false;

                // we are finished. succeed and remove the action goal handle.
                let result_msg = ExecuteScript::Result { ok: true };
                {
                    let mut ds = driver_state.lock().unwrap();
                    if let Some(mut goal) = ds.goal.take() {
                        println!("goal succeeded");
                        goal.succeed(result_msg).expect("could not set result");
                    } else {
                        println!("we fininshed but probably canceled the goal before...");
                    }
                }
            }

            if robot_state != "1" {
                // robot has entered protective stop. If there is an active goal, abort it.
                // we are finished. succeed and remove the action goal handle.
                let result_msg = ExecuteScript::Result { ok: false };
                {
                    let mut ds = driver_state.lock().unwrap();
                    if let Some(mut goal) = ds.goal.take() {
                        println!("aborting due to protective stop");
                        goal.abort(result_msg).expect("could not abort goal");
                    }
                }
            }

            // handle cancel requests
            let is_cancelling = driver_state.lock().unwrap()
                .goal.as_ref().map(|g|g.is_cancelling()).unwrap_or(false);
            if !cancelling && is_cancelling {
                // cancel and remove goal.
                cancelling = true;
                // this starts a race between completing the motion and cancelling via stopping.
                // goal removal is done by the one that succeeds first.
                let (sender, future) = oneshot::channel();
                dashboard_commands.try_send((DashboardCommand::Stop, sender)).expect("could not send...");

                let driver_state_task = driver_state.clone();
                tokio::spawn(async move {
                    if future.await.expect("failed to await cancel result") {
                        // successfully canceled.
                        println!("cancel success...");
                        let result_msg = ExecuteScript::Result { ok: false };
                        if let Some(mut goal) = driver_state_task.lock().unwrap().goal.take() {
                            goal.cancel(result_msg).expect("could not cancel goal");
                        }
                    } else {
                        println!("failed to cancel... doing nothing.");
                    }
                });
            }

            if !is_cancelling {
                cancelling = false;
            }
        }
    }
}

async fn state_publisher(
    driver_state: Arc<Mutex<DriverState>>,
    joint_publisher: Publisher<sensor_msgs::msg::JointState>,
    robot_publisher: Publisher<std_msgs::msg::String>,
    tf_prefix: String,
) -> Result<(), std::io::Error> {
    let mut clock = r2r::Clock::create(r2r::ClockType::RosTime).unwrap();
    let joint_names = vec![
                format!("{}_shoulder_pan_joint", tf_prefix),
                format!("{}_shoulder_lift_joint", tf_prefix),
                format!("{}_elbow_joint", tf_prefix),
                format!("{}_wrist_1_joint", tf_prefix),
                format!("{}_wrist_2_joint", tf_prefix),
                format!("{}_wrist_3_joint", tf_prefix),
            ];

    loop {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let now = clock.get_now().unwrap();
        let time = r2r::Clock::to_builtin_time(&now);

        let (joint_values, rob_send) = {
            let ds = driver_state.lock().unwrap();
            let rob_send = std_msgs::msg::String {
                data: if (*ds).robot_state == "1" {
                    "normal".to_owned()
                } else if (*ds).robot_state == "3" {
                    "protective".to_owned()
                } else {
                    "unknown".to_owned()
                },
            };
            ((*ds).joint_values.clone(), rob_send)
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
        robot_publisher.publish(&rob_send).unwrap();
        // println!("robot state {:?}", robot_state);
        // println!("program state {:?}", program_state);
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
        // errors can be created from strings
        return Err(Error::new(ErrorKind::Other, "oh no!"))
    }

    stream.write_all(String::from("get robot model\n").as_bytes()).await?;
    stream.flush().await?;
    let mut robot_model = String::new();
    stream.read_line(&mut robot_model).await?;
    println!("robot model: {}", robot_model);

    loop {
        let (cmd, channel) = recv.recv().await.unwrap();

        println!("dashboard writer got command {:?}", cmd);

        let (command, expected_response) =
            match cmd {
                DashboardCommand::Stop =>
                    ("stop\n", "Stopped"),
                DashboardCommand::ResetProtectiveStop =>
                    ("unlock protective stop\n", "Protective stop releasing"),
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
            println!("failed to execute command via dashboard: {}, expected: {}",
                     response, expected_response);
            if let Err(_) = channel.send(false) {
                println!("dropped dashboard return channel");
                // ignore this error for now?
            }
        }
    }
}

///
/// ur driver
///
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let ros_ctx = Context::create()?;
    let mut node = Node::create(ros_ctx, "ur_script_driver", "")?;

    let ur_address = if let Some(ParameterValue::String(s)) = node.params.get("ur_address").as_ref()
    {
        s.to_owned()
    } else {
        "192.168.2.125".to_owned()
    };

    let ur_dashboard_address = format!("{}:29999", ur_address);
    let ur_address = format!("{}:30003", ur_address);

    let tf_prefix = if let Some(ParameterValue::String(s)) = node.params.get("tf_prefix").as_ref() {
        s.to_owned()
    } else {
        "".to_owned()
    };

    let joint_publisher = node.create_publisher::<sensor_msgs::msg::JointState>("joint_states")?;
    let rob_publisher = node.create_publisher::<std_msgs::msg::String>("robot_state")?;

    let (tx, rx) = mpsc::channel::<String>(10);
    let (tx_dashboard, rx_dashboard) = mpsc::channel::<(DashboardCommand, oneshot::Sender<bool>)>(10);

    let shared_state = Arc::new(Mutex::new(DriverState::new()));

    let shared_state_cb = shared_state.clone();
    let handle_goal_cb = move |g: r2r::ServerGoal<ExecuteScript::Action>| {
        // since we already know that we do not accept goal unless we
        // don't already have one, simply set this goal handle as the
        // currently active goal...
        (*shared_state_cb.lock().unwrap()).goal.replace(g.clone());
        // ... and pass the ur script on to the driver
        tx.try_send(g.goal.script.clone())
            .expect("could not send new script");
    };

    let shared_state_cb = shared_state.clone();
    let _server = node.create_action_server::<ExecuteScript::Action>(
        "ur_script",
        Box::new(move |uuid, goal| accept_goal_cb(shared_state_cb.clone(), uuid, goal)),
        Box::new(accept_cancel_cb),
        Box::new(handle_goal_cb),
    )?;

    let task_shared_state = shared_state.clone();
    tokio::spawn(async move {
        let realtime_reader = realtime_reader(
            task_shared_state.clone(),
            tx_dashboard,
            ur_address.to_owned(),
        );
        let realtime_writer = realtime_writer(
            rx,
            ur_address.to_owned(),
        );
        let state_publisher = state_publisher(
            task_shared_state,
            joint_publisher,
            rob_publisher,
            tf_prefix.to_owned(),
        );

        let dashboard = dashboard(rx_dashboard, ur_dashboard_address.to_owned());
        tokio::try_join!(realtime_reader, realtime_writer, dashboard, state_publisher)
    });

    loop {
        node.spin_once(std::time::Duration::from_millis(8));
    }
}
