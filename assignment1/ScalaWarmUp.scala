import scala.collection.mutable.ListBuffer


object ScalaWarmUp {

	val simpleList: List[Int] = List(0,1,2,3,4,10,6)

	def main(args: Array[String]) {
		//getSecondToLast(simpleList)
		//println(getSecondToLastZip(simpleList))
		println(checkBalancedParentheses(List(")", "(", "hello", "543", ")", ")")))
	}
	


	def getSecondToLast(lst: List[Int]): Int =  lst match {

		case h :: List(x) => h

		case _ :: tail => getSecondToLast(tail)

		case _ => throw new NoSuchElementException

	}


	def getSecondToLastZip(lst: List[Int]): Int = {
		
		val temp1 = lst.zipWithIndex.filter{ case (_,v) => v == lst.size-2 }
		val temp2 = temp1.head
		val ret = (temp2._1)
		ret
		
	}

	def filterUnique(l: List[String]): List[String] = {

		val newList = scala.collection.mutable.ListBuffer.empty[String]

		for(a <- 1 to l.size-1){
			if(l(a) != l(a-1)){
				newList += l(a-1)
			}
		}
		newList += l(l.size-1)
		newList.toList
	}


	def getMostFrequentSubstring(lst: List[String], k: Int): String = {

		val mapp  = new scala.collection.mutable.HashMap[String,Int]

		for(a <- 0 to lst.size-1){
			for(b <- 0 to (lst(a).size - k )){
				if(mapp.contains((lst(a).substring(b,b+k)))){
					mapp.put((lst(a).substring(b,b+k)),mapp((lst(a).substring(b,b+k))) + 1)
				}
				else{
					mapp.put((lst(a).substring(b,b+k)),1)
				}
			}
		}
		mapp.maxBy(_._2)._1
	}


	def checkBalancedParentheses(lst: List[String]): Boolean = {

		val bitmap = lst.filter(x => (x=="(" || (x==")"))).map(x => x match{
			
			case "(" => 1
			case ")" => -1

		})
		val injectedZeroAtHead = 0::bitmap 
		val ret = injectedZeroAtHead.reduceLeft(helperFunc)
		println(ret)
		if(ret==0) true else false
  	}
	
	def helperFunc(x: Int, y: Int) : Int = { 

		(x, y) match{

			case (0, -1) => return 544
			case (x, -1) => x-1
			case (x, 1) => x +1
		}
	}
	
}