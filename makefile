TEST_FILES    := $(shell find test -name *_test.rb -type f)
REDIS_BRANCH  := unstable
TMP           := tmp
BUILD_DIR     := ${TMP}/redis-${REDIS_BRANCH}
TARBALL       := ${TMP}/redis-${REDIS_BRANCH}.tar.gz
BINARY        := ${BUILD_DIR}/src/redis-server
REDIS_TRIB    := ${BUILD_DIR}/src/redis-trib.rb
PID_PATH      := ${TMP}/redis.pid
SOCKET_PATH   := ${TMP}/redis.sock
PORT          := 6381
CLUSTER_PORTS := 7000 7001 7002 7003 7004 7005

test: ${TEST_FILES}
	make start
	make start_cluster
	env SOCKET_PATH=${SOCKET_PATH} \
		ruby -v $$(echo $? | tr ' ' '\n' | awk '{ print "-r./" $$0 }') -e ''
	make stop
	make stop_cluster

${TMP}:
	mkdir $@

${TARBALL}: ${TMP}
	wget https://github.com/antirez/redis/archive/${REDIS_BRANCH}.tar.gz -O $@

${BINARY}: ${TARBALL} ${TMP}
	rm -rf ${BUILD_DIR}
	mkdir -p ${BUILD_DIR}
	tar xf ${TARBALL} -C ${TMP}
	cd ${BUILD_DIR} && make

stop:
	(test -f ${PID_PATH} && (kill $$(cat ${PID_PATH}) || true) && rm -f ${PID_PATH}) || true

start: ${BINARY}
	${BINARY}                     \
		--daemonize  yes            \
		--pidfile    ${PID_PATH}    \
		--port       ${PORT}        \
		--unixsocket ${SOCKET_PATH}

stop_cluster: ${BINARY}
	for port in ${CLUSTER_PORTS}; do \
		(test -f ${TMP}/redis$$port.pid && (kill $$(cat ${TMP}/redis$$port.pid) || true) && rm -f ${TMP}/redis$$port.pid) || true; \
	done

start_cluster: ${BINARY}
	for port in ${CLUSTER_PORTS}; do                    \
		${BINARY}                                         \
			--daemonize            yes                      \
			--appendonly           yes                      \
			--cluster-enabled      yes                      \
			--cluster-config-file  ${TMP}/nodes$$port.conf  \
			--cluster-node-timeout 5000                     \
			--pidfile              ${TMP}/redis$$port.pid   \
			--port                 $$port                   \
			--unixsocket           ${TMP}/redis$$port.sock; \
	done
	yes yes | bundle exec ruby ${REDIS_TRIB} create \
		--replicas 1                                  \
		127.0.0.1:7000                                \
		127.0.0.1:7001                                \
		127.0.0.1:7002                                \
		127.0.0.1:7003                                \
		127.0.0.1:7004                                \
		127.0.0.1:7005

clean:
	(test -d ${BUILD_DIR} && cd ${BUILD_DIR}/src && make clean distclean) || true
	(test -f appendonly.aof && rm appendonly.aof) || true
	for port in ${CLUSTER_PORTS}; do \
		(test -f ${TMP}/nodes$$port.conf && rm ${TMP}/nodes$$port.conf) || true; \
	done

.PHONY: test start stop start_cluster stop_cluster