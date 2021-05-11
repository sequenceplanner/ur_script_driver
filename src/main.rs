use tokio::prelude::*;
use r2r::*;
use tokio::sync::mpsc::channel;
use std::sync::{Arc, Mutex};
mod driver;

#[tokio::main]
async fn main() -> io::Result<()> {
    let ros_ctx = Context::create().expect("panic");
    let mut node = Node::create(ros_ctx, "testnode", "").expect("panic");

    let ur_address = if let Some(ParameterValue::String(s)) = node.params.get("ur_address").as_ref() {
        s.to_owned()
    } else {
        "0.0.0.0:30003".to_owned()
    };

    let tf_prefix = if let Some(ParameterValue::String(s)) = node.params.get("tf_prefix").as_ref() {
        s.to_owned()
    } else {
        "".to_owned()
    };

    let joint_publisher = node.create_publisher::<sensor_msgs::msg::JointState>("joint_states").expect("asdf");
    let prog_publisher = node.create_publisher::<std_msgs::msg::String>("program_state").expect("asdf");
    let rob_publisher = node.create_publisher::<std_msgs::msg::String>("robot_state").expect("asdf");

    let program_running = Arc::new(Mutex::new(false));

    let cpr = program_running.clone();
    let (mut tx, rx) = channel::<String>(10);
    let cb2 = move |x: r2r::std_msgs::msg::String| {
        if x.data == "clear_finished".to_owned() {
            *cpr.lock().unwrap() = false;
        } else {
            tx.try_send(x.data).unwrap();
        }

    };

    let _subref = node.subscribe("urscript", Box::new(cb2)).expect("asdf");

    let rpr = program_running.clone();
    let wpr = program_running.clone();
    tokio::task::spawn(async move {
        let reader = driver::ur_reader(rpr, joint_publisher, prog_publisher, rob_publisher, ur_address.to_owned(), tf_prefix.to_owned());
        let writer = driver::ur_writer(wpr, rx, ur_address.to_owned());
        let _res = tokio::try_join!(reader, writer);
    });

    loop {
        node.spin_once(std::time::Duration::from_millis(8));
    }
}
