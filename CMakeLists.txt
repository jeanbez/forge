cmake_minimum_required(VERSION 3.10)
project(forge)

set(warnings "-Wall")
set(CMAKE_C_FLAGS "-Wall")

if(DEBUG)
	add_definitions(-DDEBUG=1)
endif(DEBUG)

find_package(MPI 3.0 REQUIRED)
find_package(GSL 2.4 REQUIRED)
include_directories(${GSL_INCLUDE_DIR})

include_directories(include)

set( FORGE_CMAKE_SOURCES
	src/main.c
	src/jsmn.c
	src/log.c
	src/fwd_list.c
	src/pqueue.c
	src/utils.c
)

add_executable(${PROJECT_NAME} ${FORGE_CMAKE_SOURCES})

target_link_libraries(${PROJECT_NAME} ${LIBS} agios)
target_link_libraries(${PROJECT_NAME} ${LIBS} gsl)
target_link_libraries(${PROJECT_NAME} ${LIBS} gslcblas)
target_link_libraries(${PROJECT_NAME} ${LIBS} m)
target_link_libraries(${PROJECT_NAME} ${LIBS} pthread)
target_link_libraries(${PROJECT_NAME} ${LIBS} explain)
target_link_libraries(${PROJECT_NAME} ${MPI_LIBRARIES})