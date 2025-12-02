# resequencing
# ------------
# Tutorial demonstrating the re-sequencing buffer pattern with LMDB persistence
# for guaranteed message ordering in BlazingMQ consumers.

# Note: The consumer uses LMDB directly, so we need a custom setup
# that also links with lmdb

project("tut_resequencing" CXX)

# Find lmdb package
find_package(lmdb CONFIG REQUIRED)

# Retrieve list of all .cpp files, and create a task for each of them
file(GLOB sources "*.cpp")
foreach (source ${sources})
  get_filename_component(taskName "${source}" NAME_WE)

  # Make sure to have a unique target name by concatenating the tutorial name
  # and the task name
  set(targetName "resequencing_${taskName}")

  # Add the executable
  add_executable(${targetName} EXCLUDE_FROM_ALL "${source}")

  # Add './' to include paths
  target_include_directories(${targetName}
                             BEFORE PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

  # Add all compile definitions used for building 'bmq'
  target_compile_options(${targetName} PRIVATE
                         "$<TARGET_PROPERTY:bmq,COMPILE_OPTIONS>")

  # Link with 'bmq' and 'lmdb'
  target_link_libraries(${targetName} PRIVATE bmq lmdb)

  # Change the name of the generated output task from
  # "<tutorialName>_<taskName>" to "<taskName>.tsk"
  set_target_properties(${targetName}
                        PROPERTIES OUTPUT_NAME "${taskName}.tsk")

  # Keep track of all targets to add the 'tutorial' rule at the end
  list(APPEND targets ${targetName})
endforeach()

# Add the 'tutorial' rule to build all tasks
add_custom_target(resequencing DEPENDS ${targets})

# Add the current tutorial to the 'tutorials' rule
add_dependencies("tutorials" resequencing)
