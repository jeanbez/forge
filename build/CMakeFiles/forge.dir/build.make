# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.10

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/jbez/Documents/Projects/forwarding-emulator

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/jbez/Documents/Projects/forwarding-emulator/build

# Include any dependencies generated for this target.
include CMakeFiles/forge.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/forge.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/forge.dir/flags.make

CMakeFiles/forge.dir/src/main.c.o: CMakeFiles/forge.dir/flags.make
CMakeFiles/forge.dir/src/main.c.o: ../src/main.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building C object CMakeFiles/forge.dir/src/main.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/forge.dir/src/main.c.o   -c /home/jbez/Documents/Projects/forwarding-emulator/src/main.c

CMakeFiles/forge.dir/src/main.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/forge.dir/src/main.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/jbez/Documents/Projects/forwarding-emulator/src/main.c > CMakeFiles/forge.dir/src/main.c.i

CMakeFiles/forge.dir/src/main.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/forge.dir/src/main.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/jbez/Documents/Projects/forwarding-emulator/src/main.c -o CMakeFiles/forge.dir/src/main.c.s

CMakeFiles/forge.dir/src/main.c.o.requires:

.PHONY : CMakeFiles/forge.dir/src/main.c.o.requires

CMakeFiles/forge.dir/src/main.c.o.provides: CMakeFiles/forge.dir/src/main.c.o.requires
	$(MAKE) -f CMakeFiles/forge.dir/build.make CMakeFiles/forge.dir/src/main.c.o.provides.build
.PHONY : CMakeFiles/forge.dir/src/main.c.o.provides

CMakeFiles/forge.dir/src/main.c.o.provides.build: CMakeFiles/forge.dir/src/main.c.o


CMakeFiles/forge.dir/src/jsmn.c.o: CMakeFiles/forge.dir/flags.make
CMakeFiles/forge.dir/src/jsmn.c.o: ../src/jsmn.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building C object CMakeFiles/forge.dir/src/jsmn.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/forge.dir/src/jsmn.c.o   -c /home/jbez/Documents/Projects/forwarding-emulator/src/jsmn.c

CMakeFiles/forge.dir/src/jsmn.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/forge.dir/src/jsmn.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/jbez/Documents/Projects/forwarding-emulator/src/jsmn.c > CMakeFiles/forge.dir/src/jsmn.c.i

CMakeFiles/forge.dir/src/jsmn.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/forge.dir/src/jsmn.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/jbez/Documents/Projects/forwarding-emulator/src/jsmn.c -o CMakeFiles/forge.dir/src/jsmn.c.s

CMakeFiles/forge.dir/src/jsmn.c.o.requires:

.PHONY : CMakeFiles/forge.dir/src/jsmn.c.o.requires

CMakeFiles/forge.dir/src/jsmn.c.o.provides: CMakeFiles/forge.dir/src/jsmn.c.o.requires
	$(MAKE) -f CMakeFiles/forge.dir/build.make CMakeFiles/forge.dir/src/jsmn.c.o.provides.build
.PHONY : CMakeFiles/forge.dir/src/jsmn.c.o.provides

CMakeFiles/forge.dir/src/jsmn.c.o.provides.build: CMakeFiles/forge.dir/src/jsmn.c.o


CMakeFiles/forge.dir/src/log.c.o: CMakeFiles/forge.dir/flags.make
CMakeFiles/forge.dir/src/log.c.o: ../src/log.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building C object CMakeFiles/forge.dir/src/log.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/forge.dir/src/log.c.o   -c /home/jbez/Documents/Projects/forwarding-emulator/src/log.c

CMakeFiles/forge.dir/src/log.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/forge.dir/src/log.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/jbez/Documents/Projects/forwarding-emulator/src/log.c > CMakeFiles/forge.dir/src/log.c.i

CMakeFiles/forge.dir/src/log.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/forge.dir/src/log.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/jbez/Documents/Projects/forwarding-emulator/src/log.c -o CMakeFiles/forge.dir/src/log.c.s

CMakeFiles/forge.dir/src/log.c.o.requires:

.PHONY : CMakeFiles/forge.dir/src/log.c.o.requires

CMakeFiles/forge.dir/src/log.c.o.provides: CMakeFiles/forge.dir/src/log.c.o.requires
	$(MAKE) -f CMakeFiles/forge.dir/build.make CMakeFiles/forge.dir/src/log.c.o.provides.build
.PHONY : CMakeFiles/forge.dir/src/log.c.o.provides

CMakeFiles/forge.dir/src/log.c.o.provides.build: CMakeFiles/forge.dir/src/log.c.o


CMakeFiles/forge.dir/src/fwd_list.c.o: CMakeFiles/forge.dir/flags.make
CMakeFiles/forge.dir/src/fwd_list.c.o: ../src/fwd_list.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building C object CMakeFiles/forge.dir/src/fwd_list.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/forge.dir/src/fwd_list.c.o   -c /home/jbez/Documents/Projects/forwarding-emulator/src/fwd_list.c

CMakeFiles/forge.dir/src/fwd_list.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/forge.dir/src/fwd_list.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/jbez/Documents/Projects/forwarding-emulator/src/fwd_list.c > CMakeFiles/forge.dir/src/fwd_list.c.i

CMakeFiles/forge.dir/src/fwd_list.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/forge.dir/src/fwd_list.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/jbez/Documents/Projects/forwarding-emulator/src/fwd_list.c -o CMakeFiles/forge.dir/src/fwd_list.c.s

CMakeFiles/forge.dir/src/fwd_list.c.o.requires:

.PHONY : CMakeFiles/forge.dir/src/fwd_list.c.o.requires

CMakeFiles/forge.dir/src/fwd_list.c.o.provides: CMakeFiles/forge.dir/src/fwd_list.c.o.requires
	$(MAKE) -f CMakeFiles/forge.dir/build.make CMakeFiles/forge.dir/src/fwd_list.c.o.provides.build
.PHONY : CMakeFiles/forge.dir/src/fwd_list.c.o.provides

CMakeFiles/forge.dir/src/fwd_list.c.o.provides.build: CMakeFiles/forge.dir/src/fwd_list.c.o


CMakeFiles/forge.dir/src/pqueue.c.o: CMakeFiles/forge.dir/flags.make
CMakeFiles/forge.dir/src/pqueue.c.o: ../src/pqueue.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Building C object CMakeFiles/forge.dir/src/pqueue.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/forge.dir/src/pqueue.c.o   -c /home/jbez/Documents/Projects/forwarding-emulator/src/pqueue.c

CMakeFiles/forge.dir/src/pqueue.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/forge.dir/src/pqueue.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/jbez/Documents/Projects/forwarding-emulator/src/pqueue.c > CMakeFiles/forge.dir/src/pqueue.c.i

CMakeFiles/forge.dir/src/pqueue.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/forge.dir/src/pqueue.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/jbez/Documents/Projects/forwarding-emulator/src/pqueue.c -o CMakeFiles/forge.dir/src/pqueue.c.s

CMakeFiles/forge.dir/src/pqueue.c.o.requires:

.PHONY : CMakeFiles/forge.dir/src/pqueue.c.o.requires

CMakeFiles/forge.dir/src/pqueue.c.o.provides: CMakeFiles/forge.dir/src/pqueue.c.o.requires
	$(MAKE) -f CMakeFiles/forge.dir/build.make CMakeFiles/forge.dir/src/pqueue.c.o.provides.build
.PHONY : CMakeFiles/forge.dir/src/pqueue.c.o.provides

CMakeFiles/forge.dir/src/pqueue.c.o.provides.build: CMakeFiles/forge.dir/src/pqueue.c.o


CMakeFiles/forge.dir/src/utils.c.o: CMakeFiles/forge.dir/flags.make
CMakeFiles/forge.dir/src/utils.c.o: ../src/utils.c
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Building C object CMakeFiles/forge.dir/src/utils.c.o"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -o CMakeFiles/forge.dir/src/utils.c.o   -c /home/jbez/Documents/Projects/forwarding-emulator/src/utils.c

CMakeFiles/forge.dir/src/utils.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/forge.dir/src/utils.c.i"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -E /home/jbez/Documents/Projects/forwarding-emulator/src/utils.c > CMakeFiles/forge.dir/src/utils.c.i

CMakeFiles/forge.dir/src/utils.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/forge.dir/src/utils.c.s"
	/usr/bin/cc $(C_DEFINES) $(C_INCLUDES) $(C_FLAGS) -S /home/jbez/Documents/Projects/forwarding-emulator/src/utils.c -o CMakeFiles/forge.dir/src/utils.c.s

CMakeFiles/forge.dir/src/utils.c.o.requires:

.PHONY : CMakeFiles/forge.dir/src/utils.c.o.requires

CMakeFiles/forge.dir/src/utils.c.o.provides: CMakeFiles/forge.dir/src/utils.c.o.requires
	$(MAKE) -f CMakeFiles/forge.dir/build.make CMakeFiles/forge.dir/src/utils.c.o.provides.build
.PHONY : CMakeFiles/forge.dir/src/utils.c.o.provides

CMakeFiles/forge.dir/src/utils.c.o.provides.build: CMakeFiles/forge.dir/src/utils.c.o


# Object files for target forge
forge_OBJECTS = \
"CMakeFiles/forge.dir/src/main.c.o" \
"CMakeFiles/forge.dir/src/jsmn.c.o" \
"CMakeFiles/forge.dir/src/log.c.o" \
"CMakeFiles/forge.dir/src/fwd_list.c.o" \
"CMakeFiles/forge.dir/src/pqueue.c.o" \
"CMakeFiles/forge.dir/src/utils.c.o"

# External object files for target forge
forge_EXTERNAL_OBJECTS =

forge: CMakeFiles/forge.dir/src/main.c.o
forge: CMakeFiles/forge.dir/src/jsmn.c.o
forge: CMakeFiles/forge.dir/src/log.c.o
forge: CMakeFiles/forge.dir/src/fwd_list.c.o
forge: CMakeFiles/forge.dir/src/pqueue.c.o
forge: CMakeFiles/forge.dir/src/utils.c.o
forge: CMakeFiles/forge.dir/build.make
forge: /usr/local/lib/libmpi.so
forge: CMakeFiles/forge.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "Linking C executable forge"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/forge.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/forge.dir/build: forge

.PHONY : CMakeFiles/forge.dir/build

CMakeFiles/forge.dir/requires: CMakeFiles/forge.dir/src/main.c.o.requires
CMakeFiles/forge.dir/requires: CMakeFiles/forge.dir/src/jsmn.c.o.requires
CMakeFiles/forge.dir/requires: CMakeFiles/forge.dir/src/log.c.o.requires
CMakeFiles/forge.dir/requires: CMakeFiles/forge.dir/src/fwd_list.c.o.requires
CMakeFiles/forge.dir/requires: CMakeFiles/forge.dir/src/pqueue.c.o.requires
CMakeFiles/forge.dir/requires: CMakeFiles/forge.dir/src/utils.c.o.requires

.PHONY : CMakeFiles/forge.dir/requires

CMakeFiles/forge.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/forge.dir/cmake_clean.cmake
.PHONY : CMakeFiles/forge.dir/clean

CMakeFiles/forge.dir/depend:
	cd /home/jbez/Documents/Projects/forwarding-emulator/build && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/jbez/Documents/Projects/forwarding-emulator /home/jbez/Documents/Projects/forwarding-emulator /home/jbez/Documents/Projects/forwarding-emulator/build /home/jbez/Documents/Projects/forwarding-emulator/build /home/jbez/Documents/Projects/forwarding-emulator/build/CMakeFiles/forge.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/forge.dir/depend

