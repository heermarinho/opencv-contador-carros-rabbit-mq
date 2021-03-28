import cv2
import numpy as np
from time import sleep
from constantes import *
from kombu import Exchange
from kombu import Producer
from kombu import Connection
import pickle
from datetime import datetime as dt 


from console_logging.console import Console
console = Console()
rabbit_url = "amqp://guest:guest@192.168.0.108:5672//"

def connect_rabbitmq():
    # Connect to RabbitMQ
    conn = Connection(rabbit_url)
    channel = conn.channel()
 
    exchange = Exchange("contador-carro-exchange", type="direct", delivery_mode=1)
    producer = Producer(
        exchange=exchange, channel=channel, routing_key="contador-carro-exchange"
    )
    
    queue = Queue(name="contador-carro-exchange", exchange=exchange, routing_key="contador-carro-exchange")
    queue.maybe_bind(conn)
    queue.declare()

    return producer



def pega_centro(x, y, largura, altura):
    """
    :param x: x do objeto
    :param y: y do objeto
    :param largura: largura do objeto
    :param altura: altura do objeto
    :return: tupla que contém as coordenadas do centro de um objeto
    """
    x1 = largura // 2
    y1 = altura // 2
    cx = x + x1
    cy = y + y1
    return cx, cy


def set_info(detec):
    global carros
    producer = connect_rabbitmq()

    for (x, y) in detec:
        if (pos_linha + offset) > y > (pos_linha - offset):
            carros += 1
            cv2.line(frame1, (25, pos_linha), (1200, pos_linha), (0, 127, 255), 3)
            detec.remove((x, y))
            console.info("Carros detectados até o momento: " + str(carros))
            data = {
                        "id_camera": "2f784c6a-4343-479b-94b8-addb52482912",
                        "data_": "/".join(str(dt.now()).split()[0].split("-")),
                        "dt": str(dt.now()).split(".")[0],
                        "evento": 1
                    }
            console.log(data)
            imagePickle = pickle.dumps(data)

                    # Catch timeout error
            try:
               producer.publish(imagePickle, content_encoding='binary')
            except TimeoutError:
               print("Timeout at ", datetime.now())
               producer = connect_rabbitmq()

 

def show_info(frame1, dilatada):
    text = f'Carros: {carros}'
   # console.log(text)
    cv2.putText(frame1, text, (450, 70), cv2.FONT_HERSHEY_SIMPLEX, 2, (0, 0, 255), 5)
    #cv2.imshow("Video Original", frame1)
    #cv2.imshow("Detectar", dilatada)


carros = caminhoes = 0
cap = cv2.VideoCapture('https://quantificacao.s3-sa-east-1.amazonaws.com/video.mp4')
subtracao = cv2.createBackgroundSubtractorMOG2()  # Pega o fundo e subtrai do que está se movendo
    # Setup

while True:
  
    ret, frame1 = cap.read()  # Pega cada frame do vídeo
    tempo = float(1 / delay)
    sleep(tempo)  # Dá um delay entre cada processamento
    grey = cv2.cvtColor(frame1, cv2.COLOR_BGR2GRAY)  # Pega o frame e transforma para preto e branco
    blur = cv2.GaussianBlur(grey, (3, 3), 5)  # Faz um blur para tentar remover as imperfeições da imagem
    img_sub = subtracao.apply(blur)  # Faz a subtração da imagem aplicada no blur
    dilat = cv2.dilate(img_sub, np.ones((5, 5)))  # "Engrossa" o que sobrou da subtração
    kernel = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (
        5, 5))  # Cria uma matriz 5x5, em que o formato da matriz entre 0 e 1 forma uma elipse dentro
    dilatada = cv2.morphologyEx(dilat, cv2.MORPH_CLOSE, kernel)  # Tenta preencher todos os "buracos" da imagem
    dilatada = cv2.morphologyEx(dilatada, cv2.MORPH_CLOSE, kernel)

    contorno, img = cv2.findContours(dilatada, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    cv2.line(frame1, (25, pos_linha), (1200, pos_linha), (255, 127, 0), 3)
    for (i, c) in enumerate(contorno):
        (x, y, w, h) = cv2.boundingRect(c)
        validar_contorno = (w >= largura_min) and (h >= altura_min)
        if not validar_contorno:
            continue

        cv2.rectangle(frame1, (x, y), (x + w, y + h), (0, 255, 0), 2)
        centro = pega_centro(x, y, w, h)
        detec.append(centro)
        cv2.circle(frame1, centro, 4, (0, 0, 255), -1)

    set_info(detec)
    show_info(frame1, dilatada)

    if cv2.waitKey(1) == 27:
        break

cv2.destroyAllWindows()
cap.release()