add_executable(
    MediaService
    MediaService.cpp
)

target_include_directories(
    MediaService PRIVATE
    /usr/local/include/jaegertracing
)

target_link_libraries(
    MediaService
    nlohmann_json::nlohmann_json
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
)

install(TARGETS MediaService DESTINATION ./)