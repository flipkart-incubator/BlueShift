package com.flipkart.fdp.migration.FSclients;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ftp.FTPFileSystem;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

/**
* Created by sushil.s on 28/08/15.
*/
@Getter
@Setter
public class MultiFTPClient extends FTPFileSystem{

    URL ftpURI = null;

    public MultiFTPClient(URI uri, Configuration conf) throws IOException {
        super.initialize(uri,conf);
        ftpURI = uri.toURL();
    }

    public String getHostAddress() {
        return ftpURI.getHost();
    }

    public OutputStream getOutputStream() throws IOException {
        //"ftp://user:pass@ftp.something.com/file.txt;type=i";
        System.out.println("FTP URL : "+ftpURI.toString());
        return ftpURI
                .openConnection()
                .getOutputStream();
    }


}
