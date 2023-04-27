cmake_minimum_required(VERSION 3.5)
project(ur_script_driver)

find_package(ament_cmake REQUIRED)

include(r2r_cargo.cmake)

# put ros package dependencies here.
r2r_cargo(sensor_msgs
          std_msgs
          ur_script_msgs
          rcl
          rcl_action
         )

# install binaries
install(PROGRAMS
  ${CMAKE_SOURCE_DIR}/target/colcon/${PROJECT_NAME}
  DESTINATION lib/${PROJECT_NAME}
)

# we need this for ros/colcon
ament_package()
