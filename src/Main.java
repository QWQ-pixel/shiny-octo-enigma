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
import java.util.Objects;
import java.util.Scanner;

public class Main { // имена файлов не содержит пути и слеша
    //− mkdir <имя каталога в HDFS> (создание каталога в HDFS); +
    //− put <имя локального файла> (загрузка файла в HDFS); +
    //− get <имя файла в HDFS> (скачивание файла из HDFS); +
    //− append <имя локального файла> <имя файла в HDFS> (конкатенация
    //файла в HDFS с локальным файлом); +
    //− delete <имя файла в HDFS> (удаление файла в HDFS); +
    //− ls (отображение содержимого текущего каталога в HDFS с разделением
    //файлов и каталогов); +
    //− cd <имя каталога в HDFS> (переход в другой каталог в HDFS, ".." — на
    //уровень выше); ?
    //− lls (отображение содержимого текущего локального каталога с
    //разделением файлов и каталогов); +
    //− lcd <имя локального каталога> (переход в другой локальный каталог,
    //".." — на уровень выше). ?
    public static void main(String[] args) {
        try {

            Configuration conf = new Configuration (); // создаем экземпляр объекта конфигурации
            URI uri = new URI("hdfs://localhost:9000");
            FileSystem fs = FileSystem.get (uri, conf, "NIto"); // Получить объект файловой системы, три параметра соответственно привязаны к uri, conf, текущей учетной записи пользователя
            Scanner keyboard = new Scanner(System.in);
            String [] com = keyboard.toString().split(" ");

            switch (com[0]){
                case "mkdir":
                    createDir(com[1], fs);
                case "put":
                    uploadFile(com[1], fs);
                case "get":
                    getFile(com[1], com[2], fs);
                case "append":
                    appendFiles(com[1], com[2], fs);
                case "delete":
                    deleteFile(com[1], fs);
                case "ls":
                    getLDInfo();
                case "cd":
                    cd(com[1], fs);
                case "lls":
                    list(new Path("/"+com[1]), fs);
                case "lcd":
                    lcd(com[1]);
            }

        }catch (Exception e) {
            e.printStackTrace();
        }
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
            String dstDir = "/user/NIto"; // каталог пользователя hdfs
            Path src = new Path (filePath); // Привязать путь пути
            Path dst = new Path(dstDir);

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
                System.out.println("File not found");

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
    public static void createDir(String nameDir, FileSystem fileSystem) {
        Path path = new Path(nameDir);
        try{
            if (fileSystem.exists(path)) {
                return;
            }
            fileSystem.mkdirs(path);

        }catch (IOException e){
            e.printStackTrace();
        }

    }
    public static void list(Path path, FileSystem fileSystem) throws IOException { //выводим список файлов и каталогов new Path("/")
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for(FileStatus file:fileStatuses){
            if(file.isFile()){
                System.out.println(file.getPath().toString()); // сюда файл
            }else{ //сюда каталог
                String nameDir = file.getPath().toString();
                String[] parse = nameDir.split("/");
                nameDir = parse[parse.length-1];
                System.out.println(nameDir);
            }
        }
    }
    public static void getLDInfo(){ // get local dir info
        File dir = new File(System.getProperty("user.dir")); //локальная директория
        File [] files = dir.listFiles(); // получаем содержимое
        for(File item : files){ // сортируем где файлы, а где папка
            if(item.isDirectory()){
                System.out.println(item.getName() + "\t folder");
            }
            else{
                System.out.println(item.getName() + "\t file");
            }
        }
    }
    public static void lcd(String dir){
        if(Objects.equals(dir, "..")){
            //переход на каталог выше
            System.out.println(" ");
        }else{
           // System.setProperty("user.dir", dir);
            System.out.println(new File(".").getAbsolutePath());
        }
    }
    public static void cd(String dir, FileSystem fs){ //переход в другую дир hdfs
        if(Objects.equals(dir, "..")){
            //переход на каталог выше
            System.out.println(fs.getHomeDirectory());
        }else{
            // System.setProperty("user.dir", dir);
            System.out.println(new File(".").getAbsolutePath());
        }
    }
}
