import org.apache.spark.{SparkConf, SparkContext}
import java.io.PrintWriter

import scala.util.control._

object peiwen_du_task1 {
  def main(args: Array[String]): Unit = {

    val fiter_number = args(0).toInt
    val inputfile = args(1)
    val betweenness_file =args(2)
    val community_file = args(3)

//    val fiter_number = 7
//    val inputfile = "/Users/peiwendu/Downloads/public_data/sample_data.csv"
//    val betweenness_file ="betweenness.txt"
//    val community_file = "community1.txt"

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("inf553_hw4")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val start = System.currentTimeMillis()
    val sample = sc.textFile(inputfile)
    val head = sample.first()
    val user_buiness_in_sample = sample.filter(_!=head).map(x=>x.split(","))
    val users = user_buiness_in_sample.map(_(0)).distinct().collect()
    val businesses = user_buiness_in_sample.map(_(1)).distinct().collect()
    val user_index = scala.collection.mutable.HashMap.empty[String, Int]
    val business_index = scala.collection.mutable.HashMap.empty[String, Int]
    var index_of_users = 0
    var index_of_business = 0
    for (e <- users) {
      user_index(e) = index_of_users
      index_of_users += 1
    }
    for (e<- businesses){
      business_index(e) = index_of_business
      index_of_business+=1
    }

//     construct graph
    val graph = user_buiness_in_sample.map(x=>(business_index(x(1)),List(user_index(x(0))))).reduceByKey((x,y)=>List.concat(x,y)).flatMap(x=>{
      val pairs = scala.collection.mutable.ListBuffer.empty[Tuple2[Tuple2[Int,Int],Int]]
      for (pair<-x._2.combinations(2)) {
        val a = pair.min
        val b = pair.max
        pairs+=Tuple2(Tuple2(a,b),1)
      }
       pairs
    }).reduceByKey((x,y)=>x+y).filter(_._2>=fiter_number).map(_._1)

    val nodes = graph.flatMap(x=>List(x._1,x._2)).distinct()

    val childnodes = graph.flatMap(x=>List((x._1,List(x._2)),(x._2,List(x._1)))).reduceByKey((x,y)=>List.concat(x,y)).collect()
    val parent_child = scala.collection.mutable.HashMap.empty[Int,List[Int]]
    for (node <-childnodes){
      parent_child(node._1) = node._2
    }

    val n = childnodes.length

//    println(nodes.count())
//    for (edge<-nodes.collect()){
//      println(edge)
//    }
//

    val betweenness = nodes.flatMap(x=>{
      val res = scala.collection.mutable.ListBuffer.empty[Tuple2[Tuple2[Int,Int],Double]]
      if (parent_child.contains(x)){
        val occurence_nodes = scala.collection.mutable.ListBuffer.empty[Int]
        val father_of_node = scala.collection.mutable.HashMap.empty[Int,List[Int]]
        val path_num = scala.collection.mutable.HashMap.empty[Int,Int]
        val layers = scala.collection.mutable.ListBuffer.empty[scala.collection.mutable.ListBuffer[Int]]
        var last_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
        last_layer_nodes.append(x)
        path_num(x) = 1
        occurence_nodes.append(x)
        layers.append(last_layer_nodes)

        val loop = new Breaks
        loop.breakable{
          while (occurence_nodes.length <= n){
            val now_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
            for (node<-last_layer_nodes){
              val child_nodes = parent_child(node)
              for (cn<-child_nodes){
                if (now_layer_nodes.contains(cn)){
                  father_of_node(cn) ++= List(node)
                  path_num(cn) += path_num(node)
                }
                else{
                  if (!occurence_nodes.contains(cn)){
                    now_layer_nodes.append(cn)
                    father_of_node(cn) = List(node)
                    path_num(cn) = path_num(node)
                    occurence_nodes.append(cn)
                  }
                }
              }
            }
            if (now_layer_nodes.isEmpty){
              loop.break
            }
            last_layer_nodes = now_layer_nodes
            layers.insert(0,last_layer_nodes)
          }
        }
//        println("BFS")
        val child_of_node = scala.collection.mutable.HashMap.empty[Int,List[Int]]
        val node_value = scala.collection.mutable.HashMap.empty[Int,Double]
        val edge_betweenness_in_each_graph = scala.collection.mutable.HashMap.empty[Tuple2[Int,Int],Double]
        val loop2 = new Breaks
        loop2.breakable{
          for (layer <- layers){
            val last_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
            for (node<-layer){
              if (!node_value.contains(node)){
                node_value(node) = 1
              }
              if (!father_of_node.contains(node)){
                loop2.break
              }
              for (fn<-father_of_node(node)){
                edge_betweenness_in_each_graph((fn, node)) = node_value(node) / path_num(node) * path_num(fn)
                if(last_layer_nodes.contains(fn)){
                  child_of_node(fn) ++=  List(node)
                }
                else{
                  last_layer_nodes.append(fn)
                  child_of_node(fn) = List(node)
                }
              }
            }
            for (fn <- last_layer_nodes){
              node_value(fn) = 1
              for (cn <- child_of_node(fn)){
                node_value(fn) += edge_betweenness_in_each_graph((fn, cn))
              }
            }
          }
        }
        for (key<- edge_betweenness_in_each_graph){
          val keys = List(key._1._1,key._1._2).sorted
          res.append(((keys(0),keys(1)),key._2))
        }
      }
      res
    }).reduceByKey((x,y)=>x+y).map(x=>(x._1,x._2/2))

    val original_betweeness = betweenness.map(x=>(List(users(x._1._1),users(x._1._2)).sorted,x._2)).sortBy(x=>x._1(0)).sortBy(x=>x._2,false)

    val writer_betweenness = new PrintWriter(betweenness_file)
    for (pair <- original_betweeness.collect()){
      writer_betweenness.println("("+pair._1(0)+", "+pair._1(1)+")"+", "+pair._2.toString)
    }
    writer_betweenness.close()

    val m = graph.count()
    val edges = graph.collect()


    val delete_edge = betweenness.sortBy(x=>x._2,false).map(x=>Tuple2(x._1._1, x._1._2)).first()
//    println(betweenness.sortBy(x=>x._2,false).first())
    val delete_edges = scala.collection.mutable.ListBuffer.empty[Tuple2[Int,Int]]
    delete_edges.append(delete_edge)
    var default_sets = sc.emptyRDD[scala.collection.mutable.ListBuffer[Int]]
    var Q_max = 0.0
    var increase = 1.0

    while (increase >= 0){
      val new_graph = graph.filter(!delete_edges.contains(_))
//      println(new_graph.count())
      val children = new_graph.flatMap(x =>List((x._1, List(x._2)), (x._2, List(x._1)))).reduceByKey((x, y)=> List.concat(x,y)).collect()
      val new_parent_child = scala.collection.mutable.HashMap.empty[Int,List[Int]]
      for (node <-children) {
        new_parent_child(node._1) = node._2
      }

      val sets = graph.filter(x=>delete_edges.contains(x)).flatMap(x=> List(x._1, x._2)).distinct().map(x=>{
        val one_set = scala.collection.mutable.ListBuffer.empty[Int]
        one_set.append(x)
        var last_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
        last_layer_nodes.append(x)
        val loop3 = new Breaks
        loop3.breakable{
          while (one_set.length <= n){
            if (!new_parent_child.contains(x)) {
              loop3.break()
            }
            val now_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
            for (node <- last_layer_nodes) {
              val child_nodes = new_parent_child(node)
              for (cn <- child_nodes) {
                if (!now_layer_nodes.contains(cn)) {
                  if (!one_set.contains(cn)) {
                    now_layer_nodes.append(cn)
                  }
                }
              }
            }
            if (now_layer_nodes.isEmpty){
              loop3.break()
            }
            last_layer_nodes = now_layer_nodes
//              println(now_layer_nodes)
            one_set ++= last_layer_nodes
          }
        }
        one_set.sorted
      }).distinct()

//      println(sets.count())

      val left_sets = nodes.subtract(sets.flatMap(x=>x)).map(x=>{
        val one_set = scala.collection.mutable.ListBuffer.empty[Int]
        one_set.append(x)
        var last_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
        last_layer_nodes.append(x)
        val loop3 = new Breaks
        loop3.breakable{
          while (one_set.length <= n){
            if (!new_parent_child.contains(x)) {
              loop3.break()
            }
            val now_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
            for (node <- last_layer_nodes) {
              val child_nodes = new_parent_child(node)
              for (cn <- child_nodes) {
                if (!now_layer_nodes.contains(cn)) {
                  if (!one_set.contains(cn)) {
                    now_layer_nodes.append(cn)
                  }
                }
              }
            }
            if (now_layer_nodes.isEmpty){
              loop3.break()
            }
            last_layer_nodes = now_layer_nodes
            //              println(now_layer_nodes)
            one_set ++= last_layer_nodes
          }
        }
        one_set.sorted
      }).distinct()

//      println(left_sets.count())

      val all_sets = sets.union(left_sets)
//      println(all_sets.flatMap(x=>x).count())

      val Q = all_sets.map(x=> {
//        println(x)
        val res = scala.collection.mutable.ListBuffer.empty[Double]
        for (i <- x.combinations(2)) {
//          println(i)
          if (edges.contains((i.min, i.max))) {
            res.append(1.0 - parent_child(i(0)).length * parent_child(i(1)).length / (2 * m).toDouble)
          }
          else {
            res.append(0.0 - parent_child(i(0)).length * parent_child(i(1)).length / (2 * m).toDouble)
          }
        }
//        println(res)
        res.sum
      }).sum() / (2 * m)

//      println(Q)

      if (Q_max <= Q){
        default_sets = all_sets
        Q_max = Q
      }
      increase = Q - Q_max

      delete_edges.append(nodes.flatMap(x=>{
        val res = scala.collection.mutable.ListBuffer.empty[Tuple2[Tuple2[Int,Int],Double]]
        if (new_parent_child.contains(x)){
          val occurence_nodes = scala.collection.mutable.ListBuffer.empty[Int]
          val father_of_node = scala.collection.mutable.HashMap.empty[Int,List[Int]]
          val path_num = scala.collection.mutable.HashMap.empty[Int,Int]
          val layers = scala.collection.mutable.ListBuffer.empty[scala.collection.mutable.ListBuffer[Int]]
          var last_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
          last_layer_nodes.append(x)
          path_num(x) = 1
          occurence_nodes.append(x)
          layers.append(last_layer_nodes)

          val loop5 = new Breaks
          loop5.breakable{
            while (occurence_nodes.length <= n){
              val now_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
              for (node<-last_layer_nodes){
                val child_nodes = new_parent_child(node)
                for (cn<-child_nodes){
                  if (now_layer_nodes.contains(cn)){
                    father_of_node(cn) ++= List(node)
                    path_num(cn) += path_num(node)
                  }
                  else{
                    if (!occurence_nodes.contains(cn)){
                      now_layer_nodes.append(cn)
                      father_of_node(cn) = List(node)
                      path_num(cn) = path_num(node)
                      occurence_nodes.append(cn)
                    }
                  }
                }
              }
              if (now_layer_nodes.isEmpty){
                loop5.break
              }
              last_layer_nodes = now_layer_nodes
              layers.insert(0,last_layer_nodes)
            }
          }
          val child_of_node = scala.collection.mutable.HashMap.empty[Int,List[Int]]
          val node_value = scala.collection.mutable.HashMap.empty[Int,Double]
          val edge_betweenness_in_each_graph = scala.collection.mutable.HashMap.empty[Tuple2[Int,Int],Double]
          val loop2 = new Breaks
          loop2.breakable{
            for (layer <- layers){
              val last_layer_nodes = scala.collection.mutable.ListBuffer.empty[Int]
              for (node<-layer){
                if (!node_value.contains(node)){
                  node_value(node) = 1
                }
                if (!father_of_node.contains(node)){
                  loop2.break
                }
                for (fn<-father_of_node(node)){
                  edge_betweenness_in_each_graph((fn, node)) = node_value(node) / path_num(node) * path_num(fn)
                  if(last_layer_nodes.contains(fn)){
                    child_of_node(fn) ++=  List(node)
                  }
                  else{
                    last_layer_nodes.append(fn)
                    child_of_node(fn) = List(node)
                  }
                }
              }
              for (fn <- last_layer_nodes){
                node_value(fn) = 1
                for (cn <- child_of_node(fn)){
                  node_value(fn) += edge_betweenness_in_each_graph((fn, cn))
                }
              }
            }
          }
          for (key<- edge_betweenness_in_each_graph){
            val keys = List(key._1._1,key._1._2).sorted
            res.append(((keys(0),keys(1)),key._2))
          }
        }
        res
      }).reduceByKey((x,y)=>x+y).sortBy(x=>x._2, false).map(x=>(x._1._1,x._1._2)).first())
//      for (edge<-delete_edges){
      ////        println(edge)
      ////      }
    }

    val communities = default_sets.map(x=>{
      val res = scala.collection.mutable.ListBuffer.empty[String]
      for (u<-x){
        res.append(users(u))
      }
      res.sorted
    }).sortBy(x=>x(0)).sortBy(x=>x.length).collect()

    val writer_community = new PrintWriter(community_file)
    for (c <- communities){
      for (u<- c){
        if (c.indexOf(u) != c.length-1){
          writer_community.print(u+", ")
        }
        else{
          writer_community.print(u+"\n")
        }
      }
    }
    writer_community.close()

//    val end = System.currentTimeMillis()
//    println(end - start)
  }
}
