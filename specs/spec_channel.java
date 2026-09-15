/**
 le Channel est la liaison crée par le Broker lors d'un rdv accept/connect pour permettre aux deux taches
 qui souhaitent communiquer, de communiquer
 Un channel a deux extremités et fonctionne dans les deux sens

 Les methodes peuvent etre appelées par deux taches en meme temps sauf si c'est fait du meme 
 coté de la liaison

 
 
 Le channel ne fait pas de synchronisation !
 */

abstract class Channel {

    
    /*lit dans le Channel et ecrit dans bytes[] bytes a partir de offset un message de length octets et retourne le nombre d'octets lus*/
    int read(byte[] bytes, int offset, int length);

    /*ecrit dans le Channel les octets de byte bytes a partir de offset un message de length octets et retourne le nombre d'octets ecrits*/
    int write(byte[] bytes, int offset, int length);

    /*deconnecte la liaison */
    void disconnect();

    /*renvoie un booleen vrai si deconecté et faux si connecté */
    boolean disconnected();
}