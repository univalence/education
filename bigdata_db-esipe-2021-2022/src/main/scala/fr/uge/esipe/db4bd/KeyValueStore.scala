package fr.uge.esipe.db4bd

trait KeyValueStore[K, V] {

  def put(key: K, value: V): Unit
  def get(key: K): V
  def delete(key: K): Unit
}
