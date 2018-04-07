val x = "Test scala worksheet!"
val arr = x.split(" ").map(x => x.toUpperCase)
arr.foreach(println(_))