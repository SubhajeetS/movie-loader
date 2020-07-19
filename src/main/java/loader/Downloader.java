package loader;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

public class Downloader {

	public static void download(String urlStr, String fileName) throws IOException {
		URL url = new URL(urlStr);
		BufferedInputStream bis = new BufferedInputStream(url.openStream());
		FileOutputStream fis = new FileOutputStream(fileName);
		byte[] buffer = new byte[1024];
		int count = 0;
		while ((count = bis.read(buffer, 0, 1024)) != -1) {
			fis.write(buffer, 0, count);
		}
		fis.close();
		bis.close();
		
		System.out.println("The file " +  fileName + " was downloaded successfully!");

	}

	public static void unzip(String compressedFile, String decompressedFile) throws IOException {

		byte[] buffer = new byte[1024];

		FileInputStream fileIn = new FileInputStream(compressedFile);

		GZIPInputStream gZIPInputStream = new GZIPInputStream(fileIn);

		FileOutputStream fileOutputStream = new FileOutputStream(decompressedFile);

		int bytes_read;

		while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {

			fileOutputStream.write(buffer, 0, bytes_read);
		}

		gZIPInputStream.close();
		fileOutputStream.close();

		System.out.println("The file " +  decompressedFile + " was decompressed successfully!");

	}

}
