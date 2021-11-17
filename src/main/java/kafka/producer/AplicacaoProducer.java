package kafka.producer;
import lombok.extern.slf4j.Slf4j;
import kafka.eventos.ProdutorEvento;

@Slf4j
public class AplicacaoProducer {

    public static void main(String[] args) {
        AplicacaoProducer aplicacaoProducer = new AplicacaoProducer();
        aplicacaoProducer.iniciar();
    }

    private void iniciar(){
        log.info("Iniciando a aplicação");
        ProdutorEvento produtorEvento = new ProdutorEvento();
        produtorEvento.executar();

    }

}
