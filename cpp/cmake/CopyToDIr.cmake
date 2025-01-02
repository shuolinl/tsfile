# CopyToDir.cmake

# This function is used to copy files to a directory and it will handle relative paths automatically.
function(copy_to_dir)
    set(INCLUDE_EXPORT_DR ${LIBRARY_INCLUDE_DIR} CACHE INTERNAL "Include export directory")
    foreach(file ${ARGN})
        get_filename_component(file_name ${file} NAME)
        get_filename_component(file_path ${file} PATH)
        string(REPLACE "${CMAKE_SOURCE_DIR}/src" "" relative_path "${file_path}")
        add_custom_target(
            copy_${file_name} ALL
            COMMAND ${CMAKE_COMMAND} -E make_directory ${INCLUDE_EXPORT_DR}/${relative_path}
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${file} ${INCLUDE_EXPORT_DR}/${relative_path}/${file_name}
            COMMENT "Copying ${file_name} to ${INCLUDE_EXPORT_DR}/${relative_path}"
        )
    endforeach()
endfunction()


