/*
 * Copyright 2012 LinkedIn Corp.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.viewer.reportal;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.executor.mail.MailCreator;
import azkaban.project.Project;
import azkaban.reportal.util.IStreamProvider;
import azkaban.reportal.util.ReportalHelper;
import azkaban.reportal.util.ReportalUtil;
import azkaban.reportal.util.StreamProviderHDFS;
import azkaban.security.commons.HadoopSecurityManager;
import azkaban.utils.EmailMessage;
import azkaban.webapp.AzkabanWebServer;

public class ReportalMailCreator implements MailCreator {
	public static AzkabanWebServer azkaban = null;
	public static HadoopSecurityManager hadoopSecurityManager = null;
	public static String outputLocation = "";
	public static String outputFileSystem = "";
	public static String reportalStorageUser = "";
	public static File reportalMailDirectory;
	public static final String REPORTAL_MAIL_CREATOR = "ReportalMailCreator";
	public static final int NUM_PREVIEW_ROWS = 50;

	static {
		DefaultMailCreator.registerCreator(REPORTAL_MAIL_CREATOR, new ReportalMailCreator());
	}

	@Override
	public boolean createFirstErrorMessage(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		Set<String> emailList = new HashSet<String>(option.getFailureEmails());

		return createEmail(flow, emailList, message, "Failure", azkabanName, clientHostname, clientPortNumber, false);
	}

	@Override
	public boolean createErrorEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		Set<String> emailList = new HashSet<String>(option.getFailureEmails());

		return createEmail(flow, emailList, message, "Failure", azkabanName, clientHostname, clientPortNumber, false);
	}

	@Override
	public boolean createSuccessEmail(ExecutableFlow flow, EmailMessage message, String azkabanName, String clientHostname, String clientPortNumber, String... vars) {

		ExecutionOptions option = flow.getExecutionOptions();
		Set<String> emailList = new HashSet<String>(option.getSuccessEmails());
		
		return createEmail(flow, emailList, message, "Success", azkabanName, clientHostname, clientPortNumber, true);
	}
	
	private boolean createEmail(ExecutableFlow flow, Set<String> emailList, EmailMessage message, String status, String azkabanName, String clientHostname, String clientPortNumber, boolean printData) {
		
		Project project = azkaban.getProjectManager().getProject(flow.getProjectId());

		if (emailList != null && !emailList.isEmpty()) {
			message.addAllToAddress(emailList);
			message.setMimeType("text/html");
			message.setSubject("Report " + status + ": " + project.getMetadata().get("title"));
			String urlPrefix = "https://" + clientHostname + ":" + clientPortNumber + "/reportal";
			try {
				return createMessage(project, flow, message, urlPrefix, printData);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return false;
	}

	private boolean createMessage(Project project, ExecutableFlow flow, EmailMessage message, String urlPrefix, boolean printData) throws Exception {
		message.println("<html>");
		message.println("<head></head>");
		message.println("<body style='font-family: verdana; color: #000000; background-color: #cccccc; padding: 20px;'>");
		message.println("<div style='background-color: #ffffff; border: 1px solid #aaaaaa; padding: 20px;-webkit-border-radius: 15px; -moz-border-radius: 15px; border-radius: 15px;'>");
		// Title
		message.println("<b>" + project.getMetadata().get("title") + "</b>");
		message.println("<div style='font-size: .8em; margin-top: .5em; margin-bottom: .5em;'>");
		// Status
		message.println(flow.getStatus().name());
		// Link to logs
		message.println("(<a href='" + urlPrefix + "?view&logs&id=" + flow.getProjectId() + "&execid=" + flow.getExecutionId() + "'>Logs</a>)");
		// Link to Data
		message.println("(<a href='" + urlPrefix + "?view&id=" + flow.getProjectId() + "&execid=" + flow.getExecutionId() + "'>Result data</a>)");
		// Link to Edit
		message.println("(<a href='" + urlPrefix + "?edit&id=" + flow.getProjectId() + "'>Edit</a>)");
		message.println("</div>");
		message.println("<div style='margin-top: .5em; margin-bottom: .5em;'>");
		// Description
		message.println(project.getDescription());
		message.println("</div>");

		if (printData) {
			String locationFull = (outputLocation + "/" + flow.getExecutionId()).replace("//", "/");

			IStreamProvider streamProvider = ReportalUtil.getStreamProvider(outputFileSystem);

			if (streamProvider instanceof StreamProviderHDFS) {
				StreamProviderHDFS hdfsStreamProvider = (StreamProviderHDFS) streamProvider;
				hdfsStreamProvider.setHadoopSecurityManager(hadoopSecurityManager);
				hdfsStreamProvider.setUser(reportalStorageUser);
			}

			// Get file list
			String[] fileList = ReportalHelper.filterCSVFile(streamProvider.getFileList(locationFull));
			
			// Sort files in execution order.
			// File names are in the format {EXECUTION_ORDER}-{QUERY_TITLE}.csv
			// E.g.: 1-queryTitle.csv
			Arrays.sort(fileList, new Comparator<String>() {
				public int compare(String a, String b) {
					Integer aExecutionOrder = Integer.parseInt(a.substring(0, a.indexOf('-')));
					Integer bExecutionOrder = Integer.parseInt(b.substring(0, b.indexOf('-')));
					return aExecutionOrder.compareTo(bExecutionOrder);
				}
				
				public boolean equals(Object obj) {
					return this.equals(obj);
				}
			});

			File tempFolder = new File(reportalMailDirectory + "/" + flow.getExecutionId());
			tempFolder.mkdirs();

			for (String file : fileList) {
				String filePath = locationFull + "/" + file;
				InputStream csvInputStream = null;
				OutputStream tempOutputStream = null;
				File tempOutputFile = new File(tempFolder, file);
				tempOutputFile.createNewFile();
				try {
					csvInputStream = streamProvider.getFileInputStream(filePath);
					tempOutputStream = new BufferedOutputStream(new FileOutputStream(tempOutputFile));

					IOUtils.copy(csvInputStream, tempOutputStream);
				} finally {
					IOUtils.closeQuietly(tempOutputStream);
					IOUtils.closeQuietly(csvInputStream);
				}
			}

			try {
				streamProvider.cleanUp();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			boolean emptyResults = true;

			for (String file : fileList) {
				message.println("<div style='margin-top: 10px; margin-bottom: 10px; border-bottom: 1px solid #ccc; padding-bottom: 5px; font-weight: bold;'>");
				message.println(file);
				message.println("</div>");
				message.println("<div>");
				message.println("<table border='1' cellspacing='0' cellpadding='2' style='font-size: 14px;'>");
				File tempOutputFile = new File(tempFolder, file);
				InputStream csvInputStream = null;
				try {
					csvInputStream = new BufferedInputStream(new FileInputStream(tempOutputFile));
					Scanner rowScanner = new Scanner(csvInputStream);
					int lineNumber = 0;
					while (rowScanner.hasNextLine() && lineNumber <= NUM_PREVIEW_ROWS) {
						emptyResults = false;
						String csvLine = rowScanner.nextLine();
						String[] data = csvLine.split("\",\"");
						message.println("<tr>");
						for (String item : data) {
							message.println("<td>" + item.replace("\"", "") + "</td>");
						}
						message.println("</tr>");
						if (lineNumber == NUM_PREVIEW_ROWS && rowScanner.hasNextLine()) {
							message.println("<tr>");
							message.println("<td colspan=\"" + data.length + "\">...</td>");
							message.println("</tr>");
						}
						lineNumber++;
					}
					rowScanner.close();
					message.println("</table>");
					message.println("</div>");
				} finally {
					IOUtils.closeQuietly(csvInputStream);
				}
				message.addAttachment(file, tempOutputFile);
			}
			
			// Don't send an email if there are no results, unless this is an unscheduled run.
			String unscheduledRun = flow.getExecutionOptions().getFlowParameters().get("reportal.unscheduled.run");
			boolean isUnscheduledRun = unscheduledRun != null && unscheduledRun.trim().equalsIgnoreCase("true");
			if (emptyResults && !isUnscheduledRun) {
				return false;
			}
		}

		message.println("</div>").println("</body>").println("</html>");

		return true;
	}
}
