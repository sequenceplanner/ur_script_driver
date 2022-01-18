use futures::stream::StreamExt;
use r2r::ur_script_msgs::action::ExecuteScript;
use std::time::Duration;

#[tokio::test]
async fn test_script() -> Result<(), Box<dyn std::error::Error>> {
    let ur_script = format!("def script():\n  movej([0.0,0.0,0.0,-1.62,-1.57,0.0], a=0.5, v=0.5)\nend\n\nscript()\n");
    let ur_script2 = format!("def script():\n  movej([-0.44,-0.75,1.16,-1.9,-1.57,0.0], a=0.5, v=0.5)\nend\n\nscript()\n");

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

        let mut s = &ur_script;
        loop {
            if s == &ur_script {
                s = &ur_script2;
            } else {
                s = &ur_script;
            }
            let goal = ExecuteScript::Goal { script: s.to_string() };
            println!("sending goal: {:?}", goal);
            let res = client
                .send_goal_request(goal)
                .expect("could not send goal request")
                .await;

            if let Ok((goal, result, feedback)) = res {
                println!("goal accepted: {}", goal.uuid);
                // process feedback stream in its own task
                let nested_goal = goal.clone();
                tokio::spawn(feedback.for_each(move |msg| {
                        let nested_goal = nested_goal.clone();
                        async move {
                            println!(
                                "new feedback msg {:?} -- {:?}",
                                msg,
                                nested_goal.get_status()
                            );
                        }
                    }));

                if rand::random::<bool>() { // && rand::random::<bool>() {
                    // move a bit before sending cancel.
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    let r = goal.cancel().expect("could not send cancel request").await;
                    if let Ok(()) = r {
                        println!("goal cancelled successfully.");
                    } else {
                        println!("failed to cancel goal: {:?}", r);
                    }
                }

                // await result in this task
                match result.await {
                    Ok((status, msg)) => {
                        println!("got result {} with msg {:?}", status, msg);
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
