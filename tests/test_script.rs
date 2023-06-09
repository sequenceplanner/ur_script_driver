use futures::stream::StreamExt;
use r2r::ur_script_msgs::action::ExecuteScript;
use std::time::Duration;

#[tokio::test]
async fn test_script() -> Result<(), Box<dyn std::error::Error>> {
    let ur_script1 = r#"def script():
  movej([0.0,0.0,0.0,-1.62,-1.57,0.0], a=0.20, v=0.25)
  return True
end
"#.to_string();

    let ur_script2 = r#"def script():
  movej([-0.44,-0.75,1.16,-1.9,-1.57,0.0], a=0.2, v=0.25)
  return True
end
"#.to_string();

    let ur_script3 = r#"
def script():
  socket_send_line("sleeping 1...", "ur_driver_socket")
  sleep(1.0)
  socket_send_line("sleeping 2...", "ur_driver_socket")
  sleep(1.0)
  socket_send_line("sleeping 3...", "ur_driver_socket")
  sleep(1.0)
  socket_send_line("sleeping 4...", "ur_driver_socket")
  sleep(1.0)
  socket_send_line("sleeping 5...", "ur_driver_socket")
  sleep(1.0)
  return True
end
"#.to_string();

    let ctx = r2r::Context::create()?;
    let mut node = r2r::Node::create(ctx, "testnode", "")?;
    let client = node.create_action_client::<ExecuteScript::Action>("/ur_script")?;
    let action_server_available = node.is_available(&client)?;

    tokio::spawn(async move {
        println!("waiting for action service...");
        action_server_available
            .await
            .expect("could not await action server");
        println!("action service available.");

        let mut s = &ur_script1;
        loop {
            let goal = ExecuteScript::Goal { script: s.to_string() };
            println!("***************************************");
            println!("sending goal");
            let res = client
                .send_goal_request(goal)
                .expect("could not send goal request")
                .await;

            if let Ok((goal, result, feedback)) = res {
                println!("goal accepted: {}", goal.uuid);
                // process feedback stream in its own task
                tokio::spawn(feedback.for_each(move |msg| {
                        async move {
                            println!("got feedback msg: {}", msg.feedback);
                        }
                    }));

                if rand::random::<bool>() && rand::random::<bool>() {
                    println!("Will cancel this action...");
                    // move a bit before sending cancel.
                    tokio::time::sleep(Duration::from_millis(2500)).await;
                    println!("Sending cancel now...");
                    let r = goal.cancel().expect("could not send cancel request").await;
                    if let Ok(()) = r {
                        println!("goal cancelled successfully.");
                    } else {
                        println!("failed to cancel goal: {:?}", r);
                    }
                    // wait a bit when cancelling to see effect
                    tokio::time::sleep(Duration::from_millis(5000)).await;
                }

                // await result in this task
                match result.await {
                    Ok((status, msg)) => {
                        println!("Got result {}: {}", status, msg.ok);

                        // if success we change ur program.
                        if status == r2r::GoalStatus::Succeeded && msg.ok {
                            if s == &ur_script1 {
                                s = &ur_script2;
                            } else if s == &ur_script2 {
                                s = &ur_script3;
                            } else {
                                s = &ur_script1;
                            }
                        } else {
                            println!("Execution failed, running the same program again...");
                        }
                    }
                    Err(e) => println!("action failed: {:?}", e),
                }
            } else {
                println!("goal rejected by server");
                // wait a bit to give server time to recover
                tokio::time::sleep(Duration::from_millis(2000)).await;
            }
        }
    });

    let handle = tokio::task::spawn_blocking(move || loop {
        node.spin_once(Duration::from_millis(100));
    });

    handle.await?;

    Ok(())
}
