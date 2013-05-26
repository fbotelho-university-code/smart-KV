gnome-terminal --window -x sh -c "java -cp bin/BFT-SMaRt.jar:lib/slf4j-api-1.5.8.jar:lib/slf4j-jdk14-1.5.8.jar:lib/netty-3.1.1.GA.jar:lib/commons-codec-1.5.jar bftsmart.demo.bftmap.BFTMapServer 0"
sleep 1;
gnome-terminal --window -x sh -c "java -cp bin/BFT-SMaRt.jar:lib/slf4j-api-1.5.8.jar:lib/slf4j-jdk14-1.5.8.jar:lib/netty-3.1.1.GA.jar:lib/commons-codec-1.5.jar bftsmart.demo.bftmap.BFTMapServer 1"
sleep 1;
gnome-terminal --window -x sh -c "java -cp bin/BFT-SMaRt.jar:lib/slf4j-api-1.5.8.jar:lib/slf4j-jdk14-1.5.8.jar:lib/netty-3.1.1.GA.jar:lib/commons-codec-1.5.jar bftsmart.demo.bftmap.BFTMapServer 2"
sleep 1;
gnome-terminal --window -x sh -c "java -cp bin/BFT-SMaRt.jar:lib/slf4j-api-1.5.8.jar:lib/slf4j-jdk14-1.5.8.jar:lib/netty-3.1.1.GA.jar:lib/commons-codec-1.5.jar bftsmart.demo.bftmap.BFTMapServer 3"
