
proxy:
	cargo ENGINE_LABEL_VALUE=tiflash OPENSSL_NO_VENDOR=1 OPENSSL_STATIC=1 cargo build -p raftstore_proxy

