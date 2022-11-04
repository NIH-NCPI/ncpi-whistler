FROM golang:1.13

RUN apt-get -y update
RUN apt-get -y install openjdk-11-jre
# RUN apt-get -y install libprotoc17
RUN apt-get -y install protobuf-compiler
RUN apt-get -y install libclang1-11
RUN git clone https://github.com/GoogleCloudPlatform/healthcare-data-harmonization

WORKDIR /go/healthcare-data-harmonization
RUN ls -l
RUN sh build_all.sh

RUN ln -s /go/healthcare-data-harmonization/mapping_engine/main/main /go/bin/whistle

WORKDIR /go

RUN apt-get -y install libffi6 
RUN apt-get -y install libffi-dev
RUN apt-get -y install python3.7
RUN apt-get -y install python3-pip
RUN pip3 install --upgrade pip
RUN alias python=python3.7
RUN wget https://bootstrap.pypa.io/get-pip.py

RUN pip3 install PyYAML
RUN pip3 install git+https://git@github.com/ncpi-fhir/ncpi-fhir-utility.git
RUN pip3 install git+https://github.com/ncpi-fhir/ncpi-fhir-client
RUN pip3 install git+https://github.com/NIH-NCPI/ncpi-whistler

#WORKDIR /go
#RUN which play





# ENTRYPOINT ["whistle"]

#CMD pwd
#RUN $MAIN/main --help
#ENTRYPOINT ["./main"]
