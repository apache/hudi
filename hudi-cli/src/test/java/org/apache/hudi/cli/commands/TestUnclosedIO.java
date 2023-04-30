package org.apache.hudi.cli.commands;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestUnclosedIO {

  @Test
  void testUnclosedIO() throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt"));

    System.out.print("Please enter a line of text:");
    String line = reader.readLine();
    writer.write(line);
    System.out.println("Written to output.txt");
  }
}
