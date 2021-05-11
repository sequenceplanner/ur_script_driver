use std::sync::Mutex;
use std::sync::Arc;
use tokio::prelude::*;
use r2r::*;

fn read_f64(slice: &[u8]) -> f64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(slice);
    f64::from_be_bytes(bytes)
}

pub async fn ur_reader(program_running: Arc<Mutex<bool>>,
                       joint_publisher: Publisher<sensor_msgs::msg::JointState>,
                       program_publisher: Publisher<std_msgs::msg::String>,
                       robot_publisher: Publisher<std_msgs::msg::String>,
                       ur_address: String,
                       tf_prefix: String) -> io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(&ur_address).await?;
    let mut checking_for_1 = false;
    let mut finished = false;
    let mut clock = r2r::Clock::create(r2r::ClockType::RosTime).unwrap();

    loop {
        let mut size_bytes = [0u8; 4];
        stream.read_exact(&mut size_bytes).await?;
        let msg_size = u32::from_be_bytes(size_bytes) as usize;

        // need to subtract the 4 we already read
        let mut buf: Vec<u8> = Vec::new(); buf.resize(msg_size - 4, 0);
        stream.read_exact(&mut buf).await?;

        if msg_size == 1116 {
            // let time = read_f64(&buf[0..8]);
            // println!("t: {:.1}s", time);
            let mut joints = vec!();
            let mut speeds = vec!();
            for i in 0..6 {
                let index = 248+i*8;
                let joint_val = read_f64(&buf[index..index+8]);
                joints.push(joint_val);
            }

            for i in 0..6 {
                let index = 296+i*8;
                let joint_speed = read_f64(&buf[index..index+8]);
                speeds.push(joint_speed);
            }
            let robot_state = read_f64(&buf[808..816]).to_string();
            // println!("robot state {:?}", robot_state);
            let program_state = read_f64(&buf[1048..1056]).to_string();
            // println!("program state {:?}", program_state);

            if !*program_running.lock().unwrap() {
                finished = false;
            }

            if *program_running.lock().unwrap() && program_state == "2" {
                checking_for_1 = true;
            }

            if checking_for_1 && program_state == "1" {
                checking_for_1 = false;
                finished = true;
                if !speeds.iter().all(|x| x.abs() == 0.0) {
                    panic!("Speed is not 0 and finished!")
                }
            }

            // println!("{:?}", speeds);

            let prog_send = std_msgs::msg::String {
                data: if finished  { "finished".to_owned() }
                else if *program_running.lock().unwrap() && !checking_for_1 && program_state == "1" {
                    "FAKE exeucting".to_owned()
                } else if program_state == "2" || program_state == "0" {
                    "REAL exeucting".to_owned()
                }
                else if !speeds.iter().all(|x| x.abs() == 0.0) {"VELOCITY executing".to_owned()}
                else { "idle".to_owned() }
            };

            let rob_send = std_msgs::msg::String {
                data: if robot_state == "1" {"normal".to_owned()}
                else if robot_state == "3" {"protective".to_owned()}
                else {"unknown".to_owned()}
            };

            let now = clock.get_now().unwrap();
            let time = r2r::Clock::to_builtin_time(&now);

            let header = std_msgs::msg::Header {
                stamp: time,
                ..Default::default()
            };
            let to_send = sensor_msgs::msg::JointState {
                header, // dummy
                position: joints,
                name: vec!(format!("{}_shoulder_pan_joint", tf_prefix),
                           format!("{}_shoulder_lift_joint", tf_prefix),
                           format!("{}_elbow_joint", tf_prefix),
                           format!("{}_wrist_1_joint", tf_prefix),
                           format!("{}_wrist_2_joint", tf_prefix),
                           format!("{}_wrist_3_joint", tf_prefix)
                ),
                ..Default::default()
            };

            joint_publisher.publish(&to_send).unwrap();
            robot_publisher.publish(&rob_send).unwrap();
            program_publisher.publish(&prog_send).unwrap();
        }
    }
}

pub async fn ur_writer(program_running: Arc<Mutex<bool>>,
                       mut recv: tokio::sync::mpsc::Receiver<std::string::String>,
                       ur_address: String) -> io::Result<()> {
    loop {
        let data = recv.recv().await.unwrap();
        *program_running.lock().unwrap() = true;

        let mut stream = tokio::net::TcpStream::connect(&ur_address).await?;
        if data == "protective" {
            let data = "def code():\n  protective_stop()\nend\n".to_string();
            stream.write_all(data.as_bytes()).await?;
        } else {
            stream.write_all(data.as_bytes()).await?;
        }
        stream.flush().await?;
    }
}
