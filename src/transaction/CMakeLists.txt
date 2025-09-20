set(SOURCES transaction_manager_finals.cpp)
add_library(transaction STATIC ${SOURCES})
target_link_libraries(transaction system pthread)
