.PHONY: default
default: all

%:
	$(MAKE) -C src $@

