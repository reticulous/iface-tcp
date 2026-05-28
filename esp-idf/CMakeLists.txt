file(GLOB TCP_SRCS "src/*.cpp")

idf_component_register(
    SRCS ${TCP_SRCS}
    INCLUDE_DIRS "include"
    REQUIRES
        spangap-core
        spangap-net
        reticulous-core
        lwip
)
