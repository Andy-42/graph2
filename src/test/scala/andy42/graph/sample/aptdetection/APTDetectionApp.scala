package andy42.graph.sample.aptdetection

import zio.ZIOAppDefault

trait APTDetectionApp extends ZIOAppDefault:

  // Currently using only the first 1000 lines of the original file to limit test runtime
  val filePrefix = "src/test/scala/andy42/graph/sample/aptdetection"
  //  val endpointPath = s"$filePrefix/endpoint-first-1000.json"
  val endpointPath = s"$filePrefix/endpoint.json"
