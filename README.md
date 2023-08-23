# PhalaData
Python Phala Block Chain Monitor


Building:

#pools build
docker build -t jimagarcia/phalapools -f Dockerfile-pools ./
docker push jimagarcia/phalapools

#accounts build
docker build -t jimagarcia/phalaaccounts -f Dockerfile-accounts ./
docker push jimagarcia/phalaaccounts

#Process build
docker build -t jimagarcia/phaladata -f Dockerfile-processBlocks ./
docker push jimagarcia/phaladata


#Events build
docker build -t jimagarcia/phaladataevents -f Dockerfile-events ./
docker push jimagarcia/phaladataevents

#EventItems build
docker build -t jimagarcia/phaladataeventitems -f Dockerfile-eventItem ./
docker push jimagarcia/phaladataeventitems


#Hourly build
docker build -t jimagarcia/eventhourly -f Dockerfile-hourly ./
docker push jimagarcia/eventhourly

#processPools build
docker build -t jimagarcia/processpools -f Dockerfile-processPools ./
docker push jimagarcia/processpools



#processEvents build
docker build -t jimagarcia/processevents -f Dockerfile-processEvents ./
docker push jimagarcia/processevents


EventItemsBack build
docker build -t jimagarcia/phaladataeventitemsback -f Dockerfile-eventItemsBack ./
docker push jimagarcia/phaladataeventitemsback



run local
docker run --name phaladata jimagarcia/phaladata


temp
eventblockfirst build
docker build -t jimagarcia/eventblockfirst -f Dockerfile-eventBlockFirst ./
docker push jimagarcia/eventblockfirst
