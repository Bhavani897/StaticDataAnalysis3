package com.rite.products.convertrite.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlFileParser {

	private static final Logger log = LoggerFactory.getLogger(ControlFileParser.class);
	// private String controlFilePath;
	// private int tableNameLinePosition = 6;
	private int columnNameLinePosition = 8;
	private List<String> controlFileContents;

	// File f=new
	// File("C:\\Users\\ADMIN\\Documents\\controlfiles\\PozSupThirdPartyInt.ctl");

	public ControlFileParser(String controlFilePath) throws Exception {

		log.info(controlFilePath + "::::::controlFilePath");
		File f = new File(controlFilePath);
		// controlFilePath = _controlFilePath;
		if (f.exists()) {
			log.debug("entering ifffff");
			controlFileContents = Files.readAllLines(f.toPath(), Charset.defaultCharset());
			//log.debug(controlFileContents + ":::::::controlFileContents");
			columnNameLinePosition = getLineNumber(controlFilePath);
		}

	}

	/*
	 * // Process Table Name public String GetTableName() { if
	 * (this.controlFileContents.size() > 0) { String[] splittedTableLine =
	 * controlFileContents.get(tableNameLinePosition).trim().split(" "); if
	 * (splittedTableLine.length == 3) { return
	 * splittedTableLine[2].replace("_BSTG", ""); } }
	 * 
	 * return "";
	 * 
	 * }
	 */

	public int getLineNumber(String controlFilePath) throws Exception {
		LineNumberReader lineNumberReader = null;
		int lineNumber = 0;
		try {
			// Construct the LineNumberReader object
			lineNumberReader = new LineNumberReader(new FileReader(controlFilePath));
			// Read all lines now; Every read increase the line number by 1
			String line = null;
			while ((line = lineNumberReader.readLine()) != null) {
				if (line.toUpperCase().contains("TRAILING NULLCOLS"))
					lineNumber = lineNumberReader.getLineNumber();
			}
		} catch (Exception e) {
			throw new Exception(e.getMessage());
		} finally {
			// Close the LineNumberReader
			try {
				if (lineNumberReader != null) {
					lineNumberReader.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		return lineNumber;
	}

	public List<String> getColumnNames() {
		ArrayList<String> columnNames = new ArrayList<>();
		ArrayList<String> columnLines = new ArrayList<String>(
				controlFileContents.stream().skip(columnNameLinePosition).collect(Collectors.toList()));
		// Skip columns until we reach the column name line
		// System.out.println(columnLines);a
		columnLines.remove(columnLines.size() - 1); // Remove the last line ')'
		for (String columnLine : columnLines) {
			String tempColumnLine = columnLine.trim().replace(",", "");
			if (!(tempColumnLine.toLowerCase().contains("constant")
					|| tempColumnLine.toLowerCase().contains("expression"))) {
				// System.out.println(tempColumnLine.trim());
				// String[] realColumnNames = tempColumnLine.trim().split(" ");
				String[] realColumnNames = tempColumnLine.trim().split("\\s+");
				// System.out.println(realColumnNames[0]+"realColumnNames");
				if (realColumnNames[0].contains("char"))
					realColumnNames[0] = realColumnNames[0].substring(0, realColumnNames[0].indexOf("char"));
				else if (realColumnNames[0].contains("\"decode"))
					realColumnNames[0] = realColumnNames[0].substring(0, realColumnNames[0].indexOf("\"decode"));
				else if (realColumnNames[0].contains("\"trim"))
					realColumnNames[0] = realColumnNames[0].substring(0, realColumnNames[0].indexOf("\"trim"));
				else if (realColumnNames[0].contains("("))
					realColumnNames[0] = realColumnNames[0].substring(1);
				else if (realColumnNames[0].equalsIgnoreCase(")") || realColumnNames[0].equalsIgnoreCase("(")) {
					realColumnNames[0] = "";
				}else if (realColumnNames[0].startsWith("--")) {
					realColumnNames[0] = "";
				}

				if (!realColumnNames[0].isEmpty() && realColumnNames != null)
					columnNames.add(realColumnNames[0].replaceAll("\\s+", ""));
			}
		}
		// System.out.println(columnNames);
		return columnNames;
	}

}
