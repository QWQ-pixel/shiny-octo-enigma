import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;

public class Main { // имена файлов не содержит пути и слеша
    private static Client client;
    public static void main(String[] args) {
        // localhost 9000 NIto
        try {
            Scanner keyboard = new Scanner(System.in);
            System.out.println("Введите через пробел сервер, порт и имя пользователя: ");
            String input = keyboard.nextLine();
            String[] com = input.toString().split(" ");
            if (com.length == 3) {
                client = new Client(com[0], com[1], com[2]);
                System.out.println("Добро пожаловать в HDFS! Введите команду help для просмотра команд");
                System.out.println("Локальная директория: "+client.getLocalDir());
                System.out.println("Директория HDFS: "+client.getHdfsDir());
                while (true) {
                    input = keyboard.nextLine();
                    com = input.toString().split(" ");
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
                            getFile(com[1], com[2], client.getFs());
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
                "lcd <имя локального каталога> (переход в другой локальный каталог, \"..\" — на уровень выше)"};
        System.out.println("------------------------------------------------------------------------");

        for (String s : h)
            System.out.println(s);

        System.out.println("------------------------------------------------------------------------");

    }
    public static void appendFiles(String localFile, String hdfsFile, FileSystem fs) throws IOException { // в теории должно сработать
        FSDataInputStream in = fs.open(new Path(hdfsFile)); //грузим файл с hdfs
        OutputStream out = new FileOutputStream(localFile); // берем локальный файл в который будем заливать
        int b = 0;
        byte[] buf = new byte[1 << 20];
        while ( (b = in.read(buf)) >= 0)
            out.write(buf, 0, b);
        in.close(); // закрываем потоки
        out.close();
        // add some notification that all ok
    }
    public static void uploadFile(String filePath, FileSystem fileSystem){
        try{
            //надо к установленной лкальной папке аппендить имя файла
            Path src = new Path(filePath); // Привязать путь пути
            Path dst = new Path(client.getHdfsDir().toUri()); //переписать


            /*загрузить файлы*/
            fileSystem.copyFromLocalFile(src, dst); // Вызов команды copyFromLocal
            fileSystem.close (); // Закрываем объект файловой системы
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("File uploaded!");
    }
    public static void deleteFile(String fileName, FileSystem fs) {
        //передаем файл в ф ю проверяем удаляем
        try{
            Path file = new Path(fileName);

            if (!fs.exists(file))
                fs.delete(file, true);
            else
                System.out.println("Файл не найден!");

        }catch(IOException e){
            e.printStackTrace();
        }

    }
    public static void getFile(String fileName, String localPath, FileSystem fileSystem){ //пересмотреть ф ю
        //проверяем на наличие выводим если есть если нет выводим что нет
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(new Path(fileName));
            OutputStream out = new FileOutputStream(localPath); //куда будем скачивать скорее всего предустановим директорию
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(in != null){
                IOUtils.closeStream(in);
            }
        }
    }
    public static void createDir(String nameDir, FileSystem fileSystem) { // отрабатывает норм
        Path path = new Path(nameDir);
        try{
            if (fileSystem.exists(path)) {
                return;
            }
            fileSystem.mkdirs(path);
            System.out.println(" ");
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public static void ls(Path path, FileSystem fileSystem) throws IOException { //выводим список файлов и каталогов
        FileStatus[] fileStatuses = fileSystem.listStatus(path); // добавить обработку пустых папок
            for (FileStatus file : fileStatuses) {
                if (file.isFile()) {
                    System.out.println(file.getPath().toString()); // сюда файл
                } else { //сюда каталог
                    String nameDir = file.getPath().toString();
                    String[] parse = nameDir.split("/");
                    nameDir = parse[parse.length - 1];
                    System.out.println(nameDir);
                }
            }
    }
    public static void lls(String path){ // local норм отрабатывает
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
    public static void lcd(String dir){ //local кажется отрабатывает нормально
        StringBuilder newDir = new StringBuilder();
        if(Objects.equals(dir, "..")){
            //переход на каталог выше
            String[] path = dir.split("/");
            if(path.length == 1){
                newDir = new StringBuilder("/" + path[0]);
                client.setLocalDir(new Path(newDir.toString()));
            }else {

                for(int i = 0; i < path.length-1; i++)
                    newDir.append(path[i]);

                client.setLocalDir(new Path(newDir.toString()));
            }
        }else{
            newDir.append("/").append(dir);
            client.setLocalDir(new Path(newDir.toString()));
            //System.out.println(new File(".").getAbsolutePath());
        }
    }
    public static void cd(String dir, FileSystem fs){ //переход в другую дир hdfs
        StringBuilder newDir = new StringBuilder();
        if(Objects.equals(dir, "..")){
            //переход на каталог выше
            String[] path = dir.split("/");
            if(path.length == 1){
                newDir = new StringBuilder("/" + path[0]);
                client.setHdfsDir(new Path(newDir.toString()));
            }else {

                for(int i = 0; i < path.length-1; i++)
                    newDir.append(path[i]);

                client.setHdfsDir(new Path(newDir.toString()));
            }
        }else{
            newDir.append("/").append(dir);
            client.setHdfsDir(new Path(newDir.toString()));
        }
    }
    static class Client{
        private Path localDir;
        private Path hdfsDir;
        private final FileSystem fs;
        public Client(String server, String port, String name) throws URISyntaxException, IOException, InterruptedException {
            Configuration conf = new Configuration(); // создаем экземпляр объекта конфигурации
            StringBuilder sb = new StringBuilder("hdfs://");
            String path = String.valueOf(sb.append(server).append(':').append(port));
            URI uri = new URI(path);
            fs = FileSystem.get(uri, conf, name); // Получить объект файловой системы, три параметра соответственно привязаны к uri, conf, текущей учетной записи пользователя
            localDir = new Path("/home/"+name); // local dir
            hdfsDir = fs.getHomeDirectory();
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
    }
}
