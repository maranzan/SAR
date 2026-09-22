/**
 * un broker est un objet qui fait le lien entre deux tâches distantes, en leur 
 * fournissant un canal de communication.
 */

abstract class Broker {

  /**
   * Crée un broker nommé name, immédiatement utilisable.
   * @throws IllegalArgumentException si un broker de même nom existe déjà.
   */
  Broker(String name){}

  /**
   * Attend une connexion entrante sur le port donné de ce broker.
   * Bloquant : ne retourne que lorsqu'un connect(nom de ce broker, port)
   * a été effectué. Retourne le canal connecté à la tâche distante.
   *
   * @throws IllegalArgumentException si port < 0
   * @throws IllegalStateException si un accept est déjà en attente sur ce port
   */
    public abstract Channel accept(int port);

  /**
   * Se connecte au port donné du broker nommé name.
   * - Si aucun broker de ce nom n'existe : retourne null immédiatement.
   * - Sinon, bloquant : attend qu'un accept soit effectué sur ce port,
   *   puis retourne le canal connecté.
   
   *
   * @throws IllegalArgumentException si port < 0
   */
  public abstract Channel connect(String name, int port);
}