import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

public class Main {
    private static Client client;
    public static void main(String[] args) {
        // localhost 9000 NIto
        try {
            Scanner keyboard = new Scanner(System.in);
            System.out.println("Введите через пробел сервер, порт и имя пользователя: ");
            String input = keyboard.nextLine();
            String[] com = input.split(" ");
            if (com.length == 3) {
                client = new Client(com[0], com[1], com[2]);
                System.out.println("Добро пожаловать в HDFS! Введите команду help для просмотра команд");
                while (true) {
                    input = keyboard.nextLine();
                    com = input.split(" ");
                    switch (com[0]) {
                        case "help":
                            help();
                            break;
                        case "mkdir":
                            createDir(com[1], client.getFs());
                            break;
                        case "put":
                            uploadFile(com[1], client.getFs());
                            break;
                        case "get":
                            getFile(com[1], client.getDownPath(), client.getFs());
                            break;
                        case "append":
                            appendFiles(com[1], com[2], client.getFs());
                            break;
                        case "delete":
                            deleteFile(com[1], client.getFs());
                            break;
                        case "ls":
                            ls(client.getHdfsDir(), client.getFs());
                            break;
                        case "cd":
                            cd(com[1], client.getFs());
                            break;
                        case "lls":
                            lls(client.getLocalDir());
                            break;
                        case "lcd":
                            lcd(com[1]);
                            break;
                        case "exit":
                            keyboard.close();
                            client.getFs().close();
                            System.exit(1);
                            break;
                        default:
                            System.out.println("Something wrong");
                    }
                }
            }else{
                System.out.println("Нужно три параметра!");
            }

        }catch(Exception e){
                e.printStackTrace();
        }
    }
    public static void help(){
        String[] h = new String[] {"mkdir <имя каталога в HDFS> (создание каталога в HDFS)",
                "put <имя локального файла> (загрузка файла в HDFS)",
                "get <имя файла в HDFS> (скачивание файла из HDFS)",
                "append <имя локального файла> <имя файла в HDFS> (конкатенация файла в HDFS с локальным файлом)",
                "delete <имя файла в HDFS> (удаление файла в HDFS)",
                "ls (отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов)",
                "cd <имя каталога в HDFS> (переход в другой каталог в HDFS, \"..\" — на уровень выше)",
                "lls (отображение содержимого текущего локального каталога с разделением файлов и каталогов)",
                "lcd <имя локального каталога> (переход в другой локальный каталог, \"..\" — на уровень выше)",
                "exit выход"};
        System.out.println("------------------------------------------------------------------------");

        for (String s : h)
            System.out.println(s);

        System.out.println("------------------------------------------------------------------------");

    }
    public static void appendFiles(String localFile, String hdfsFile, FileSystem fs) {
        try{
            String file = client.getLocalDir()+"/"+localFile;
            InputStream in = new FileInputStream(file);
            FSDataOutputStream out = fs.append(new Path(client.getHdfsDir()+"/"+hdfsFile));
            IOUtils.copyBytes(in, out, 4096);
            out.close();
            IOUtils.closeStream(in);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void uploadFile(String filePath, FileSystem fs){
        try{
            String file = client.getLocalDir() +'/'+filePath;
            InputStream in = new FileInputStream(file);
            FSDataOutputStream out = fs.create(new Path(client.getHdfsDir()+"/"+filePath));
            int b = 0;
            byte[] buf = file.getBytes();
            while ( (b = in.read(buf)) >= 0)
                out.write(buf, 0, b);
            in.close(); // закрываем потоки
            out.close();

        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void deleteFile(String fileName, FileSystem fs) {
        try{
            Path file = new Path(client.getHdfsDir()+"/"+fileName);
            if (fs.exists(file))
                fs.delete(file, true);
            else
                System.out.println("Файл не найден!");

        }catch(IOException e){
            e.printStackTrace();
        }
    }
    public static void getFile(String fileName, String localPath, FileSystem fileSystem){
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(new Path(client.getHdfsDir()+"/"+fileName));
            OutputStream out = new FileOutputStream(localPath+"/"+fileName);
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(in != null){
                IOUtils.closeStream(in);
            }
        }
    }
    public static void createDir(String nameDir, FileSystem fs) {
        Path path = new Path(client.getHdfsDir()+"/"+nameDir);
        try{
            fs.mkdirs(path);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void ls(Path path, FileSystem fileSystem) { //выводим список файлов и каталогов
        try {
            ArrayList<String> files = new ArrayList<>();
            ArrayList<String> folders = new ArrayList<>();
            FileStatus[] fileStatuses = fileSystem.listStatus(path);
            for (FileStatus file : fileStatuses) {
                if (file.isFile()) { // сюда файл
                    String[] fileName = file.getPath().toString().split("/");
                    files.add(fileName[fileName.length-1]);
                } else { //сюда каталог
                    String nameDir = file.getPath().toString();
                    String[] parse = nameDir.split("/");
                    nameDir = parse[parse.length - 1];
                    folders.add(nameDir);
                }
            }
            if(!files.isEmpty()) {
                System.out.println("Файлы: ");
                for (String f : files)
                    System.out.println(f);
            }
            if(!folders.isEmpty()) {
                System.out.println("Папки:");
                for (String f : folders)
                    System.out.println(f);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }
    public static void lls(String path){ // local
        File dir = new File(path);
        File [] files = dir.listFiles(); // получаем содержимое
        ArrayList<String> fs = new ArrayList<>();
        ArrayList<String> folders = new ArrayList<>();
        assert files != null;
        if(files.length != 0) {
            for (File item : files) { // сортируем где файлы, а где папка
                if (item.isDirectory()) {
                    folders.add(item.getName());
                } else {
                    fs.add(item.getName());
                }
            }
            if(!fs.isEmpty()) {
                System.out.println("Файлы: ");

                for (String name : fs)
                    System.out.println(name);
            }
            if(!folders.isEmpty()) {
                System.out.println("Папки: ");

                for (String name : folders)
                    System.out.println(name);
            }
        }else{
            System.out.println("Папка пуста!");
        }
    }
    public static void lcd(String dir){
        StringBuilder newDir = new StringBuilder();
        String oldDir = client.getLocalDir();
        if(Objects.equals(dir, "..")){
            //переход на каталог выше
            String[] path = oldDir.split("/");
            for(String p : path){
                System.out.println(p);
            }
            if(path.length == 1){
                newDir = new StringBuilder('/' + path[0] + '/');
                client.setLocalDir(new Path(newDir.toString()));
            }else {

                for(int i = 0; i < path.length-1; i++) {
                    newDir.append(path[i]);
                    newDir.append('/');
                }
                client.setLocalDir(new Path(newDir.toString()));
            }
        }else{
            newDir.append(oldDir);

            if(oldDir.length() > 1)
                newDir.append('/');

            newDir.append(dir).append('/');
            System.out.println(newDir);
            client.setLocalDir(new Path(newDir.toString()));
        }
    }
    public static void cd(String dir, FileSystem fs){
        StringBuilder newDir = new StringBuilder();
        String oldDir = String.valueOf(client.getHdfsDir());
        if(Objects.equals(dir, "..")){
            //переход на каталог выше
            String[] path = oldDir.split("/");

            if(path.length == 1){
                newDir = new StringBuilder('/'+path[0]+'/');
                client.setHdfsDir(new Path(newDir.toString()));
                System.out.println(newDir);
            }else {

                for(int i = 0; i < path.length-1; i++) {
                    newDir.append(path[i]);
                    newDir.append('/');
                }
                if(newDir.toString().contains("hdfs://localhost:9000")){
                    System.out.println(newDir.toString().replace("hdfs://localhost:9000", ""));
                }else{
                    System.out.println(newDir);
                }
                client.setHdfsDir(new Path(newDir.toString()));
            }
        }else{
            newDir.append(oldDir);

            if(oldDir.length() > 1)
                newDir.append('/');

            newDir.append(dir).append('/');
            if(newDir.toString().contains("hdfs://localhost:9000")){
                System.out.println(newDir.toString().replace("hdfs://localhost:9000", ""));
            }else{
                System.out.println(newDir);
            }
            client.setHdfsDir(new Path(newDir.toString()));
        }
    }
    static class Client{
        private Path localDir;
        private Path hdfsDir;
        private final FileSystem fs;
        private final String downPath;
        public Client(String server, String port, String name) throws URISyntaxException, IOException, InterruptedException {
            Configuration conf = new Configuration(); // создаем экземпляр объекта конфигурации
            StringBuilder sb = new StringBuilder("hdfs://");
            String path = String.valueOf(sb.append(server).append(':').append(port));
            URI uri = new URI(path);
            fs = FileSystem.get(uri, conf, name); // Получить объект файловой системы, три параметра соответственно привязаны к uri, conf, текущей учетной записи пользователя
            localDir = new Path("/home/"+name);
            downPath = localDir+"/Загрузки";
            hdfsDir =  fs.getHomeDirectory();
            //new Path("/");
        }

        public String getLocalDir() {
            return localDir.toString();
        }

        public void setLocalDir(Path localDir) {
            this.localDir = localDir;
        }

        public Path getHdfsDir() {
            return hdfsDir;
        }

        public void setHdfsDir(Path hdfsDir) {
            this.hdfsDir = hdfsDir;
        }

        public FileSystem getFs() {
            return fs;
        }

        public String getDownPath() {
            return downPath;
        }
    }
}
