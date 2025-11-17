cmake_minimum_required(VERSION 3.5)
project(social_network_microservices)

find_package(libmongoc-1.0 1.13 REQUIRED)
find_package(nlohmann_json 3.5.0 REQUIRED)
find_package(Threads)
find_package(OpenSSL REQUIRED)
find_package(amqpcpp REQUIRED)

set(Boost_USE_STATIC_LIBS ON)
find_package(Boost 1.54.0 REQUIRED COMPONENTS log log_setup program_options)
if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
  link_directories(${Boost_LIBRARY_DIRS})
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "-O3")
set(CMAKE_INSTALL_PREFIX /usr/local/bin)

include("../../cmake/Findlibmemcached.cmake")

add_executable(
    UserService
    UserService.cpp
)

target_include_directories(
    UserService PRIVATE
    ${LIBMEMCACHED_INCLUDE_DIR}
    ${MONGOC_INCLUDE_DIRS}
    /usr/local/include/jwt
    /usr/local/include/jaegertracing
)

target_link_libraries(
    UserService
    ${MONGOC_LIBRARIES}
    ${LIBMEMCACHED_LIBRARIES}
    nlohmann_json::nlohmann_json
    ${CMAKE_THREAD_LIBS_INIT}
    ${Boost_LIBRARIES}
    Boost::log
    Boost::log_setup
    jaegertracing
    OpenSSL::SSL
)

install(TARGETS UserService DESTINATION ./)