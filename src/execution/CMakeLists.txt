set(SOURCES execution_manager_finals.cpp)
add_library(execution STATIC ${SOURCES})

target_link_libraries(execution system transaction planner)
