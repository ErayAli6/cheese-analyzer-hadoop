package fmi.cheeseanalyzer_ErayAli_2101321032;

import java.awt.Color;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.net.URI;
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import fmi.cheeseanalyzer_ErayAli_2101321032.mapper.*;
import fmi.cheeseanalyzer_ErayAli_2101321032.reducer.*;

public class App extends JFrame {
	private JTable resultTable;
	private DefaultTableModel tableModel;
	private JComboBox<String> provCodeCombo;
	private JComboBox<String> categoryCombo;
	private JComboBox<String> milkTypeCombo;
	private JComboBox<String> calculationType;

	public static void main(String[] args) {
		new App();
	}

	public App() {
		initUI();
	}

	private void initUI() {
		JPanel panel = new JPanel();
		panel.setLayout(null);
		setSize(800, 900);
		setTitle("Cheese Analyzer");
		getContentPane().setBackground(Color.WHITE);

		String[] manufacturerProvCodeOptions = { "All", "AB", "BC", "MB", "NB", "NL", "NS", "ON", "PE", "QC", "SK" };
		String[] categoryTypeOptions = { "All", "Firm Cheese", "Fresh Cheese", "Hard Cheese", "Semi-soft Cheese",
				"Soft Cheese", "Veined Cheese" };
		String[] milkTypeOptions = { "All", "Cow", "Goat", "Ewe", "Cow and Goat", "Cow and Ewe", "Goat and Ewe",
				"Cow, Goat and Ewe", "Buffalo Cow" };
		String[] calcTypes = { "None", "Average Moisture", "Organic Percentage" };

		provCodeCombo = new JComboBox<>(manufacturerProvCodeOptions);
		categoryCombo = new JComboBox<>(categoryTypeOptions);
		milkTypeCombo = new JComboBox<>(milkTypeOptions);
		calculationType = new JComboBox<>(calcTypes);

		JButton analyzeButton = new JButton("Analyze");
		analyzeButton.setFont(new Font("Arial", Font.BOLD, 14));
		analyzeButton.setBackground(new Color(59, 89, 182));
		analyzeButton.setForeground(Color.WHITE);
		analyzeButton.setFocusPainted(false);

		tableModel = new DefaultTableModel();
		resultTable = new JTable(tableModel);
		JScrollPane scrollPane = new JScrollPane(resultTable);

		JLabel parametersLabel = new JLabel("Analysis Parameters:");
		JLabel provLabel = new JLabel("Province:");
		JLabel catLabel = new JLabel("Category:");
		JLabel milkLabel = new JLabel("Milk Type:");
		JLabel calcLabel = new JLabel("Calculation:");
		JLabel resultsLabel = new JLabel("Analysis Results:");

		int labelWidth = 100;
		int comboWidth = 200;
		int height = 25;
		int startX = 50;
		int gapY = 40;

		parametersLabel.setBounds(startX, 20, 200, height);

		provLabel.setBounds(startX, 60, labelWidth, height);
		provCodeCombo.setBounds(startX + labelWidth + 10, 60, comboWidth, height);

		catLabel.setBounds(startX, 60 + gapY, labelWidth, height);
		categoryCombo.setBounds(startX + labelWidth + 10, 60 + gapY, comboWidth, height);

		milkLabel.setBounds(startX, 60 + 2 * gapY, labelWidth, height);
		milkTypeCombo.setBounds(startX + labelWidth + 10, 60 + 2 * gapY, comboWidth, height);

		calcLabel.setBounds(startX, 60 + 3 * gapY, labelWidth, height);
		calculationType.setBounds(startX + labelWidth + 10, 60 + 3 * gapY, comboWidth, height);

		analyzeButton.setBounds(startX + labelWidth + 10, 60 + 4 * gapY + 10, comboWidth, height + 10);

		resultsLabel.setBounds(startX, 60 + 5 * gapY + 40, 200, height);

		scrollPane.setBounds(startX, 60 + 5 * gapY + 70, 700, 550);

		panel.add(parametersLabel);
		panel.add(provLabel);
		panel.add(provCodeCombo);
		panel.add(catLabel);
		panel.add(categoryCombo);
		panel.add(milkLabel);
		panel.add(milkTypeCombo);
		panel.add(calcLabel);
		panel.add(calculationType);
		panel.add(analyzeButton);
		panel.add(resultsLabel);
		panel.add(scrollPane);

		add(panel);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setLocationRelativeTo(null);
		setVisible(true);

		analyzeButton.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				startHadoopAnalysis();
			}
		});
	}

	private void startHadoopAnalysis() {
		Configuration conf = new Configuration();
		Path inputPath = new Path("hdfs://127.0.0.1:9000/input/18.csv");
		Path outputPath = new Path("hdfs://127.0.0.1:9000/result");
		JobConf job = new JobConf(conf, App.class);

		job.set("provCode", provCodeCombo.getSelectedItem().toString());
		job.set("category", categoryCombo.getSelectedItem().toString());
		job.set("milkType", milkTypeCombo.getSelectedItem().toString());
		job.set("calcType", calculationType.getSelectedItem().toString());

		setMapperAndReducer(job);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileOutputFormat.setOutputPath(job, outputPath);
		FileInputFormat.setInputPaths(job, inputPath);

		try {
			FileSystem fs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), job);
			if (fs.exists(outputPath)) {
				fs.delete(outputPath, true);
			}
			JobClient.runJob(job);
			readResultFile(fs, job);
		} catch (IOException e) {
			e.printStackTrace();
			JOptionPane.showMessageDialog(this, "Error processing data: " + e.getMessage(), "Error",
					JOptionPane.ERROR_MESSAGE);
		}
	}

	private void readResultFile(FileSystem fs, JobConf job) throws IOException {
		Path resultFile = new Path("hdfs://127.0.0.1:9000/result/part-00000");
		if (!fs.exists(resultFile)) {
			JOptionPane.showMessageDialog(this, "Result file not found in HDFS.", "Error", JOptionPane.ERROR_MESSAGE);
			return;
		}

		try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(resultFile)))) {
			populateTableModel(br, job);
		}
	}

	private void populateTableModel(BufferedReader br, JobConf job) throws IOException {
		String calcType = job.get("calcType");
		setTableColumns(calcType);
		tableModel.setRowCount(0);

		String line;
		while ((line = br.readLine()) != null) {
			addRowToTable(line, calcType);
		}
	}

	private void setTableColumns(String calcType) {
		if (calcType.equals("None")) {
			String[] columns = { "ManufacturerProvCode", "ManufacturingTypeEn", "MoisturePercent", "FlavourEn",
					"CharacteristicsEn", "Organic", "CategoryTypeEn", "MilkTypeEn", "MilkTreatmentTypeEn", "RindTypeEn",
					"CheeseName", "FatLevel" };
			tableModel.setColumnIdentifiers(columns);
		} else if (calcType.equals("Average Moisture")) {
			String[] columns = { "Province", "Category", "Milk Type", "Average Moisture" };
			tableModel.setColumnIdentifiers(columns);
		} else if (calcType.equals("Organic Percentage")) {
			String[] columns = { "Province", "Category", "Milk Type", "Organic Percentage (%)" };
			tableModel.setColumnIdentifiers(columns);
		}
	}

	private void addRowToTable(String line, String calcType) {
		String[] parts = line.split("\t");
		if (calcType.equals("None")) {
			String keyPart = parts[0];
			String[] dataColumns = keyPart.split("-", -1);
			tableModel.addRow(dataColumns);
		} else {
			String[] filters = parts[0].split("-");
			String resultValue = parts[1];
			Object[] rowData = { filters[0], filters[1], filters[2], resultValue };
			tableModel.addRow(rowData);
		}
	}

	private void setMapperAndReducer(JobConf job) {
		String calcTypeSelected = calculationType.getSelectedItem().toString();
		switch (calcTypeSelected) {
		case "Average Moisture":
			job.setMapperClass(AverageMoistureMapper.class);
			job.setReducerClass(AverageMoistureReducer.class);
			break;
		case "Organic Percentage":
			job.setMapperClass(OrganicPercentageMapper.class);
			job.setReducerClass(OrganicPercentageReducer.class);
			break;
		default:
			job.setMapperClass(FilterMapper.class);
			job.setReducerClass(FilterReducer.class);
			break;
		}
	}
}