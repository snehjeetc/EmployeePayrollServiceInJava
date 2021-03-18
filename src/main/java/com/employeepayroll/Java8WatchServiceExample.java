package com.employeepayroll;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import static java.nio.file.StandardWatchEventKinds.*;

public class Java8WatchServiceExample {
    private final WatchService watcher;
    private final Map<WatchKey, Path> dirWatchers;

    //Creates a watchservice and registers the given director
    public Java8WatchServiceExample(Path dir) throws IOException {
        this.watcher = FileSystems.getDefault().newWatchService();
        this.dirWatchers = new HashMap<>();
        scanAndRegisterDirectories(dir);
    }

    /*Registers the given directory and all its subdirectories, with
    and all its subdirectories with the watch service*/
    private void scanAndRegisterDirectories(final Path start) throws IOException{
        Files.walkFileTree(start, new SimpleFileVisitor<Path>(){
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException{
                registerDirWatchers(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    //Registers the given directory with the watch service
    private void registerDirWatchers(Path dir) throws IOException{
        WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        dirWatchers.put(key, dir);
    }

    /*Process all events for keys queued to the watcher*/
    @SuppressWarnings({"rawtypes", "unchecked"})
    void processEvents(){
        while (true){
            WatchKey key;   //wait for the key to be signalled
            try{
                key = watcher.take();
            } catch(InterruptedException e){ return; }
            Path dir = dirWatchers.get(key);
            if(dir==null) continue;
            for(WatchEvent<?> event : key.pollEvents()){
                WatchEvent.Kind kind = event.kind();
                Path name = ((WatchEvent<Path>)event).context();
                Path child = dir.resolve(name);
                System.out.format("%s: %s\n", event.kind().name(), child);  //print out event

                //if directory is created, then register it and its sub-directories
                if(kind == ENTRY_CREATE){
                    try{
                        if(Files.isDirectory(child)) scanAndRegisterDirectories(child);
                    }catch(IOException e) { }
                }
                else if(kind.equals(ENTRY_DELETE)){
                    if(Files.isDirectory(child)) dirWatchers.remove(key);
                }
            }

            //reset key and remove from set if directory no longer accessible
            boolean valid = key.reset();
            if(!valid){
                dirWatchers.remove(key);
                if(dirWatchers.isEmpty()) break;    //all directories are inaccessible
            }
        }
    }

}
