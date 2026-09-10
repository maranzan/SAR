

/*
La tache est un thread qui est le "client" du broker

Il utilise le Broker pour se connecter puis utilise les channels pour lire et ecrire
 */

abstract class Task extends Thread {

    /*Crée une tache qui communiquera a l'aide du broker b et qui aura le comportement du runnable r */
    Task(Broker b, Runnable r);

    /*Retourne le Broker de la tache */
    static Broker getBroker();
}