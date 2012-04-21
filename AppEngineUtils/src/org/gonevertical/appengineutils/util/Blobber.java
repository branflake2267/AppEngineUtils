package org.gonevertical.appengineutils.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import org.gonevertical.appengineutils.WriteKindToBlob;

import com.google.appengine.api.blobstore.BlobKey;
import com.google.appengine.api.files.AppEngineFile;
import com.google.appengine.api.files.FileService;
import com.google.appengine.api.files.FileServiceFactory;
import com.google.appengine.api.files.FileWriteChannel;
import com.google.appengine.api.files.FinalizationException;
import com.google.appengine.api.files.GSFileOptions;
import com.google.appengine.api.files.GSFileOptions.GSFileOptionsBuilder;
import com.google.appengine.api.files.LockException;

public class Blobber {

  public FileService fileService = FileServiceFactory.getFileService();

  public AppEngineFile createBlob(String name) throws IOException {
    AppEngineFile file = fileService.createNewBlobFile("text/plain", name);
    return file;
  }
  
  public AppEngineFile createGoogleStorage(String bucketName, String name) throws IOException {
    GSFileOptionsBuilder optionsBuilder = new GSFileOptionsBuilder()
      .setBucket(bucketName)
      .setKey(name)
      .setMimeType("text/plain");

    AppEngineFile file = fileService.createNewGSFile(optionsBuilder.build());
    return file;
  }

  public FileWriteChannel open(AppEngineFile file) throws FileNotFoundException, FinalizationException, LockException, IOException {
    boolean lock = true;
    FileWriteChannel writeChannel = fileService.openWriteChannel(file, lock);
    return writeChannel;
  }

  public void write(FileWriteChannel writeChannel, String data) throws IOException {
    writeChannel.write(ByteBuffer.wrap(data.getBytes()));
  }

  public void close(FileWriteChannel writeChannel) throws IllegalStateException, IOException {
    writeChannel.closeFinally();
  }

  public BlobKey getBlobKey(AppEngineFile file) {
    BlobKey blobKey = fileService.getBlobKey(file);
    return blobKey;
  }
}
