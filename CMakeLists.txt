cmake_minimum_required(VERSION 3.10)
project(forge)

set(warnings "-Wall")
set(CMAKE_C_FLAGS "-Wall -O2 -g")

# Add the cmake folder so the FindSphinx module is found
set(CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

option(DEBUG "Enable debug messages for development purposes" OFF)
option(PVFS "Enable the PVFS interface to send the requests" OFF)
option(ORANGEFS "Enable the OrangeFS interface to send the requests" OFF)

set(PVFS OFF)
set(DEBUG ON)
set(STATISTICS OFF)
set(EXPLAIN ON)

if(PVFS)
	add_definitions(-DPVFS=1)
endif(PVFS)

if(DEBUG)
	add_definitions(-DDEBUG=1)
	set(CMAKE_BUILD_TYPE Debug)
endif(DEBUG)

if(STATISTICS)
	add_definitions(-DSTATISTICS=1)
endif(STATISTICS)

if(EXPLAIN)
	add_definitions(-DEXPLAIN=1)
endif(EXPLAIN)

add_definitions(-D_FILE_OFFSET_BITS=64)
add_definitions(-D_LARGEFILE_SOURCE=1)
add_definitions(-D__USE_LARGEFILE64=1)

find_package(MPI 3.0 REQUIRED)
include_directories(${MPI_C_INCLUDE_DIRS})
find_package(GSL 2.4 REQUIRED)
include_directories(${GSL_INCLUDE_DIR})

include_directories(include)

if(PVFS)
	include_directories(SYSTEM /opt/orangefs/include)
endif(PVFS)

if(PVFS)
	set( FORGE_CMAKE_SOURCES
		src/main.c
		src/scheduler.c
		src/jsmn.c
		src/log.c
		src/fwd_list.c
		src/pqueue.c
		src/utils.c
		src/thpool.c
		src/server/listener.c
		src/server/handler.c
		src/server/dispatcher.c
		src/handler/read.c
		src/handler/write.c
		src/dispatcher/orangefs.c
	)
else()
	set( FORGE_CMAKE_SOURCES
		src/main.c
		src/scheduler.c
		src/jsmn.c
		src/log.c
		src/fwd_list.c
		src/pqueue.c
		src/utils.c
		src/thpool.c
		src/server/listener.c
		src/server/handler.c
		src/server/dispatcher.c
		src/handler/read.c
		src/handler/write.c
		src/dispatcher/posix.c
	)
endif()

add_executable(${PROJECT_NAME} ${FORGE_CMAKE_SOURCES})

target_link_libraries(${PROJECT_NAME} ${LIBS} agios)
target_link_libraries(${PROJECT_NAME} ${LIBS} gsl)
target_link_libraries(${PROJECT_NAME} ${LIBS} pthread)
target_link_libraries(${PROJECT_NAME} ${LIBS} gslcblas)
target_link_libraries(${PROJECT_NAME} ${LIBS} m)
target_link_libraries(${PROJECT_NAME} ${LIBS} pthread)
target_link_libraries(${PROJECT_NAME} ${LIBS} cjson)

if(EXPLAIN)
	target_link_libraries(${PROJECT_NAME} ${LIBS} explain)
endif(EXPLAIN)

if(PVFS)
	target_link_libraries(${PROJECT_NAME} /opt/orangefs/lib/libpvfs2.so)
endif(PVFS)

target_link_libraries(${PROJECT_NAME} ${MPI_LIBRARIES})

install(TARGETS forge DESTINATION bin)

if (CMAKE_BUILD_TYPE MATCHES "^[Rr]elease")
	add_subdirectory(docs)
endif()
