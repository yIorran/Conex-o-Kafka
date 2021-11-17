package kafkaConsmer.eventos;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

@Slf4j
public class ConsumerEvento {

    private final KafkaConsumer<String, String> consumer;

    public ConsumerEvento() {
        consumer = criarConsumer();
    }

    private KafkaConsumer<String, String> criarConsumer(){
        if(consumer != null){
            return consumer;
        }
        Properties properties = new Properties();
        //conecta com o container do kafka
        properties.put("bootstrap.servers", "localhost:9092");
        //serializa a chave para o kafka entender a mensagem
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //serializa a classe
        properties.put("group.id", "default");

        return new KafkaConsumer<String, String>(properties);
    }

    public void executar(){

        //formatando a mensagem
        List<String> topicos = new ArrayList<>();
        topicos.add("RegistroEvento");
        consumer.subscribe(topicos);


        //motando a mensagem
        log.info("Iniciando consumer...");
        boolean continuar = true;
        while (continuar){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records){
                gravarMensagem(record.topic(), record.partition(), record.value());
                if(record.value().equals("FECHAR")){
                    continuar = false;
                }
            }
        }
        consumer.close();
    }

    private void gravarMensagem(String topico, Integer particao, String mensagem) {
        log.info("Topico:{}, Partição:{}, Mensagem:{}", topico,particao, mensagem);
    }

}
