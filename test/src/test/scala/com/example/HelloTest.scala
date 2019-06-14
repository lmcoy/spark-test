package com.example

import org.scalatest.{FlatSpec, Matchers}

class HelloTest extends FlatSpec with Matchers {

  "square" should "return a squared value" in {
    Hello.square(2) should equal (4)
  }
}
