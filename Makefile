all: clean compile

compile:
	./rebar compile
	 mkdir -p priv

clean:
	./rebar clean
	rm -rf priv/*

