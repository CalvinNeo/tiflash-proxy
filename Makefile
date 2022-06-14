
proxy:
	OPENSSL_DIR=`brew --prefix openssl@1.1` OPENSSL_DIR=`brew --prefix openssl@1.1` ENGINE_LABEL_VALUE=tiflash OPENSSL_NO_VENDOR=1 OPENSSL_STATIC=1 cargo build -p raftstore_proxy

release: proxy

debug: proxy