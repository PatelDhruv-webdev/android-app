package menuOptions;

import java.util.List;

public class PrintableTable {

  public static void printTable(String title, List<String> headers, List<List<String>> data) {
    // Calculate the maximum width for each column
    int[] columnWidths = new int[headers.size()];
    for (List<String> row : data) {
      for (int i = 0; i < headers.size(); i++) {
        int width = Math.max((row.get(i) == null ? 0 : row.get(i).length()), headers.get(i).length());
        if (width > columnWidths[i]) {
          columnWidths[i] = width;
        }
      }
    }

    String formattedTitle = "* " + title + " *";
    String underline = "=".repeat(formattedTitle.length());
    System.out.println(formattedTitle + "\n" + underline + "\n");

    if (data.size() == 0) {
      System.out.println("EMPTY!\n");
      return;
    }

    // Print the headers
    printRow(headers, columnWidths);
    printSeparator(columnWidths);

    // Print the data rows
    for (List<String> row : data) {
      printRow(row, columnWidths);
    }

    System.out.println("\n");
  }

  private static void printRow(List<String> row, int[] columnWidths) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < row.size(); i++) {
      String cell = row.get(i);
      sb.append(String.format("%-" + columnWidths[i] + "s", cell));
      sb.append("  ");
    }
    System.out.println(sb.toString());
  }

  private static void printSeparator(int[] columnWidths) {
    StringBuilder sb = new StringBuilder();
    for (int width : columnWidths) {
        sb.append("-".repeat(width)).append("  ");
    }
    System.out.println(sb.toString());

  }
}
