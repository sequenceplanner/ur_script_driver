
Ur script driver
-----

How to test:

```
ros2 action send_goal /ur_script ur_script_msgs/action/ExecuteScript '{script: "def code():\nmovej([1.0, 0.0, 0.0, 0.0, 0.0, 0.0],a=0.5,v=0.2)\nend\n"}'
```
