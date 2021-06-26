from kafka import KafkaProducer
from json import dumps

producer = KafkaProducer(bootstrap_servers=['localhost:29092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8')
                        )
kafka_topic = None
while True:
    if kafka_topic is None:
            kafka_topic = input("Digite o nome do tópico: \n")
            kafka_message = input("Digite a mensagem: \n")
            data = {'message' : kafka_message}
            producer.send(kafka_topic, value=data)
    else:
        confirm = input("Deseja continuar com o mesmo tópico?\n(Y/N)")
        if confirm == "Y" or confirm == "y":
            kafka_message = input("Digite a mensagem: \n")
            data = {'message' : kafka_message}
            producer.send(kafka_topic, value=data)
        else:
            kafka_topic = None
            pass
    
    finalizar = input("deseja enviar mais alguma mensagem?\n(Y/N)")
    if finalizar == "N" or finalizar == "n":
        print("obrigado.")
        exit(0)
    else:
        pass
