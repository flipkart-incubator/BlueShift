/*
 *
 *  Copyright 2015 Flipkart Internet Pvt. Ltd.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
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
        //"ftp://user:pass@ftp.something.com/file.txt";
        System.out.println("FTP URL : "+ftpURI.toString());
        return ftpURI
                .openConnection()
                .getOutputStream();
    }


}
