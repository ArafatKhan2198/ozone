#!/usr/bin/env sh

# Clear out class files that break intellij build.
GIT_ROOT="$(git rev-parse --show-toplevel)"

remove_file() {
    file="${GIT_ROOT}/${1}"
    trash-put -vf "$file"
}

remove_file 'hadoop-hdds/common/target/classes/org/apache/hadoop/ozone/common/ChunkBufferImplWithByteBuffer$1.class'
remove_file 'hadoop-hdds/common/target/classes/org/apache/hadoop/ozone/common/ChunkBufferImplWithByteBufferList$1.class'
remove_file 'hadoop-hdds/container-service/target/classes/org/apache/hadoop/ozone/container/keyvalue/impl/KeyValueStreamDataChannel$Buffers$1.class'
