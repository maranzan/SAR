/**
 le Channel est la liaison crée par le Broker lors d'un rdv accept/connect pour permettre aux deux taches
 qui souhaitent communiquer, de communiquer
 Un channel a deux extremités et fonctionne dans les deux sens

 Les methodes peuvent etre appelées par deux taches en meme temps sauf si c'est fait du meme 
 coté de la liaison

 si un coté de la liaison est deconnecté, l'autre en est informé par une exception ChannelDisconected
 */

abstract class Channel {

    
    /*lit dans le Channel et ecrit dans bytes[] bytes a partir de offset un message de length octets */
    int read(byte[] bytes, int offset, int length);

    /*ecrit dans le Channel les octets de byte bytes a partir de offset un message de length octets */
    int write(byte[] bytes, int offset, int length);

    /*deconnecte la liaison */
    void disconnect();

    /*renvoie un booleen vrai si deconecté et faut si connecté */
    boolean disconnected();
}