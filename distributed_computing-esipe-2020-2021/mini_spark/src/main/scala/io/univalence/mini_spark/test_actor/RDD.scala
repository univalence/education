package io.univalence.mini_spark.test_actor

sealed trait RDD[A] {
  def map[B](f: A => B): RDD[B] =
    MapRDD(this, f)

  def filter(p: A => Boolean): RDD[A] =
    FilterRDD(this, p)
}

case class ListRDD[A](l: List[A])                   extends RDD[A]
case class MapRDD[A, B](c: RDD[A], f: A => B)       extends RDD[B]
case class FilterRDD[A](c: RDD[A], p: A => Boolean) extends RDD[A]

object RDD {
  def from[A](l: List[A]): RDD[A] = ListRDD(l)

  def eval[A](c: RDD[A]): List[A] =
    c match {
      case ListRDD(l)               => l
      case FilterRDD(d, p)          => eval(d).filter(p)
      case MapRDD(d, f: (Any => A)) => eval(d).map(f)
    }

  def display[A](computation: RDD[A]): String =
    computation match {
      case ListRDD(l)      => "ListComputation"
      case MapRDD(c, f)    => display(c) + " -> Map"
      case FilterRDD(c, p) => display(c) + " -> Filter"
    }
}

object ComputationMain {
  def main(args: Array[String]): Unit = {
    val program: RDD[Shooting] =
      RDD
        .from(
          """Identifiant du lieu;Année du tournage;Type de tournage;Titre;Réalisateur;Producteur;Localisation de la scène;Code postal;Date de début;Date de fin;Coordonnée en X;Coordonnée en Y;geo_shape;geo_point_2d
            |2018-638;2018;Série TV;Shadowhunters;Matt Hastings;Froggie Production;place paul painlevé, 75005 paris;75005;2018-05-14;2018-05-14;2.34382793;48.85023357;"{""type"": ""Point"", ""coordinates"": [2.343827931290161, 48.85023357176381]}";48.8502335718,2.34382793129
            |2018-642;2018;Série TV;Munch  saison 2;frederic Berthe;EXILENE;rue la bruyère, 75009 paris;75009;2018-06-04;2018-06-05;2.3328443;48.87972754;"{""type"": ""Point"", ""coordinates"": [2.332844302194123, 48.87972754166997]}";48.8797275417,2.33284430219""".stripMargin
            .split("\n")
            .toList
        )
        .filter(l => !l.startsWith("Identifiant du lieu"))
        .map { line =>
          val fields = line.split(";")

          Shooting(fields(0), fields(1).toInt, fields(3), fields(6))
        }

    println(program)
    println(RDD.eval(program))
    println(RDD.display(program))
  }
}

case class Shooting(id: String, year: Int, title: String, location: String)
