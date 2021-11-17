package kafka.eventos;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class ProdutorEvento {

    private final Producer<String, String> producer;

    public ProdutorEvento() {
        producer = criarProducer();
    }

    private Producer<String, String> criarProducer(){
        if(producer != null){
            return producer;
        }
        Properties properties = new Properties();
        //conecta com o container do kafka
        properties.put("bootstrap.servers", "localhost:9092");
        //serializa a chave para o kafka entender a mensagem
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //serializa a classe
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");

        return new KafkaProducer<String, String>(properties);
    }

    public void executar(){

        //formatando a mensagem
        String chave = UUID.randomUUID().toString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String mensagem = sdf.format(new Date());
        mensagem += "|" + chave;
        mensagem += "|" + "olha eu ai";

        //motando a mensagem
        log.info("Iniciando envio da mensagem");
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("RegistroEvento", chave, mensagem);
        //enviando a mensagem
        producer.send(record);
        producer.flush();
        producer.close();
        log.info("Mensagem enviada com sucesso [{}]", mensagem);
    }

}
