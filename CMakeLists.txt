cmake_minimum_required (VERSION 3.9.0)
if (POLICY CMP0111)
cmake_policy(SET CMP0111 OLD)
endif ()
project (capr)
include_directories (
include
${CMAKE_SOURCE_DIR}/raimd/include
${CMAKE_SOURCE_DIR}/raikv/include
${CMAKE_SOURCE_DIR}/libdecnumber/include
${CMAKE_SOURCE_DIR}/raimd/libdecnumber/include
)
if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
add_definitions(/DPCRE2_STATIC)
if ($<CONFIG:Release>)
add_compile_options (/arch:AVX2 /GL /std:c11 /wd5105)
else ()
add_compile_options (/arch:AVX2 /std:c11 /wd5105)
endif ()
if (NOT TARGET pcre2-8-static)
add_library (pcre2-8-static STATIC IMPORTED)
set_property (TARGET pcre2-8-static PROPERTY IMPORTED_LOCATION_DEBUG ../pcre2/build/Debug/pcre2-8-staticd.lib)
set_property (TARGET pcre2-8-static PROPERTY IMPORTED_LOCATION_RELEASE ../pcre2/build/Release/pcre2-8-static.lib)
include_directories (../pcre2/build)
else ()
include_directories (${CMAKE_BINARY_DIR}/pcre2)
endif ()
if (NOT TARGET raikv)
add_library (raikv STATIC IMPORTED)
set_property (TARGET raikv PROPERTY IMPORTED_LOCATION_DEBUG ../raikv/build/Debug/raikv.lib)
set_property (TARGET raikv PROPERTY IMPORTED_LOCATION_RELEASE ../raikv/build/Release/raikv.lib)
endif ()
if (NOT TARGET raimd)
add_library (raimd STATIC IMPORTED)
set_property (TARGET raimd PROPERTY IMPORTED_LOCATION_DEBUG ../raimd/build/Debug/raimd.lib)
set_property (TARGET raimd PROPERTY IMPORTED_LOCATION_RELEASE ../raimd/build/Release/raimd.lib)
endif ()
if (NOT TARGET decnumber)
add_library (decnumber STATIC IMPORTED)
set_property (TARGET decnumber PROPERTY IMPORTED_LOCATION_DEBUG ../raimd/libdecnumber/build/Debug/decnumber.lib)
set_property (TARGET decnumber PROPERTY IMPORTED_LOCATION_RELEASE ../raimd/libdecnumber/build/Release/decnumber.lib)
endif ()
else ()
add_compile_options (-Wall -Wextra -Werror -ggdb -O3 -mavx -maes -fno-omit-frame-pointer)
if (TARGET pcre2-8-static)
include_directories (${CMAKE_BINARY_DIR}/pcre2)
endif ()
if (NOT TARGET raikv)
add_library (raikv STATIC IMPORTED)
set_property (TARGET raikv PROPERTY IMPORTED_LOCATION ../raikv/build/libraikv.a)
endif ()
if (NOT TARGET raimd)
add_library (raimd STATIC IMPORTED)
set_property (TARGET raimd PROPERTY IMPORTED_LOCATION ../raimd/build/libraimd.a)
endif ()
if (NOT TARGET decnumber)
add_library (decnumber STATIC IMPORTED)
set_property (TARGET decnumber PROPERTY IMPORTED_LOCATION ../raimd/libdecnumber/build/libdecnumber.a)
endif ()
endif ()
add_library (capr STATIC src/ev_capr.cpp)
if (CMAKE_SYSTEM_NAME STREQUAL "Windows")
link_libraries (capr raikv raimd decnumber pcre2-8-static ws2_32)
else ()
if (TARGET pcre2-8-static)
link_libraries (capr raikv raimd decnumber pcre2-8-static -lcares -lpthread -lrt)
else ()
link_libraries (capr raikv raimd decnumber -lpcre2-8 -lcares -lpthread -lrt)
endif ()
endif ()
add_definitions(-DCAPR_VER=1.3.0-26)
add_executable (capr_server src/server.cpp)
