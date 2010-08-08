java -XX:+AggressiveOpts -XX:+UseCompressedOops -Xmx9G -Xms9G -cp lib/test.jar:../libs/guava-r04.jar org.andrewhitchcock.duwamish.example.BenchmarkSuite | grep bestTime
