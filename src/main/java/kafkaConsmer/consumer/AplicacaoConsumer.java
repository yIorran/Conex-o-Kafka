package kafkaConsmer.consumer;
import kafkaConsmer.eventos.ConsumerEvento;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AplicacaoConsumer {

    public static void main(String[] args) {
        AplicacaoConsumer aplicacaoProducer = new AplicacaoConsumer();
        aplicacaoProducer.iniciar();
    }

    private void iniciar(){
        log.info("Iniciando a aplicação");
        ConsumerEvento produtorEvento = new ConsumerEvento();
        produtorEvento.executar();

    }

}
