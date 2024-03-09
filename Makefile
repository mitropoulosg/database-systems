hp:
	@echo " Compile hp_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/hp_main.c ./src/record.c ./src/hp_file.c -lbf -o ./build/hp_main -g3

bf:
	@echo " Compile bf_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/bf_main.c ./src/record.c -lbf -o ./build/bf_main -g3;

ht:
	@echo " Compile ht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/ht_main.c ./src/record.c ./src/ht_table.c -lbf -o ./build/ht_main -g3

sht:
	@echo " Compile sht_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/sht_main.c ./src/record.c ./src/sht_table.c ./src/ht_table.c -lbf -o ./build/sht_main -g3
stats:
	@echo " Compile stats_main ...";
	gcc -I ./include/ -L ./lib/ -Wl,-rpath,./lib/ ./examples/stats_main.c ./src/record.c ./src/sht_table.c ./src/ht_table.c -lbf -o ./build/stats_main -g3