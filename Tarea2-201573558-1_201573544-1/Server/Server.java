import java.io.*; 
import java.text.*; 
import java.util.*; 
import java.net.*; 
import java.nio.file.*; 
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.io.FileWriter;
import java.io.BufferedWriter;

public class Server  
{ 
    public static void main(String[] args) throws IOException  
    { 
        Scanner scn = new Scanner(System.in); 
        ServerSocket ss = new ServerSocket(6868); 
        System.out.println("Ingrese cantidad de maquinas");
        String cantmaquinasrecibidas = scn.nextLine();
        int totalmaquinas = Integer.parseInt(cantmaquinasrecibidas);
        File dir2 = new File("index");
        dir2.mkdir();
        for (int i =1;i<=totalmaquinas;i++){
            File dir = new File("maquina"+i);
            dir.mkdir();
        }
        while (true){ 
            Socket s = null; 
            try{    //-----------------CONEXION CON CLIENTE-------------------------
                System.out.println("Conectado / Esperando a cliente"); 
                s = ss.accept();        //  ESPERA CONEXION CON CLIENTE
                //------------------------------> COMIENZO ESCRIBIR LOG
                DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                Date date = new Date();
                Charset utf8 = StandardCharsets.UTF_8;
                try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) 
                {
                    writer.write(dateFormat.format(date) + "  ---  CONECTION  ---  " + s.getRemoteSocketAddress().toString() +" ---  coneccion entrante "+"\n");
                } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                //------------------------------> FIN ESCRIBIR LOG
                System.out.println("Nuevo cliente conectado : " + s); 
                DataInputStream dis = new DataInputStream(s.getInputStream()); 
                DataOutputStream dos = new DataOutputStream(s.getOutputStream()); 
                System.out.println("Asignando thread"); 
                Thread t = new ClientHandler(s, dis, dos, cantmaquinasrecibidas);   //ASIGNACION DE THREAD
                t.start(); 
            } 
            catch (Exception e){ 
                s.close(); 
                e.printStackTrace(); 
            } 
        } 
    } 
} 
  
class ClientHandler extends Thread  
{ 
    final DataInputStream dis; 
    final DataOutputStream dos; 
    final Socket s; 
    final String cantmaquinasrecibidas;
      
    public ClientHandler(Socket s, DataInputStream dis, DataOutputStream dos, String cantmaquinasrecibidas)  
    { 
        this.s = s; 
        this.dis = dis; 
        this.dos = dos;
        this.cantmaquinasrecibidas=cantmaquinasrecibidas;
    } 
  
    @Override
    public void run()  
    { 
        String received; 
        String toreturn; 
        while (true)    //WHILE DE ESCUCHA CONSTANTE
        { 
            try { //---------------------- RECIBIR ORDEN DE CLIENTE -----------------
                received = dis.readUTF();
                String dividir [];
                dividir = received.split(" "); 
                //-------------------- ORDEN DE SALIDA ----------------------------
                if(received.equals("exit")) 
                {  
                    System.out.println("Cliente" + this.s + "esta cerrando su conexion "); 
                    this.s.close(); 
                    //------------------------------> COMIENZO ESCRIBIR LOG
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Date date = new Date();
                    Charset utf8 = StandardCharsets.UTF_8;
                    try (Writer writer = new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) {
                        writer.write(dateFormat.format(date) + "  ---  CONECTION  ---  " + s.getRemoteSocketAddress().toString() +" ---  terminando conexion "+"\n");
                    } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                    System.out.println("conexion terminada"); 
                    //------------------------------> FIN ESCRIBIR LOG
                    break; 
                } 
                   
                switch (dividir[0]) { //dividir[0] contiene 1ra parte de input cliente (ls, get, put, delete)
                    //-----------------------------FUNCION LS ------------------------
                    case "ls" :
                        StringBuilder stringBuilder = new StringBuilder();
                        File curDir = new File("./index");
                        File[] filesList = curDir.listFiles();  
                        for(File f : filesList){        //RECORRER ARCHIVOS REGISTRADOS EN INDEX (./index/[archivos])
                            int flag = 1;
                            FileReader fr = new FileReader(f);
                            BufferedReader bf = new BufferedReader(fr);
                            int cantidadPartes = Integer.parseInt(bf.readLine());
                            int machine;
                            String sintxt1[];
                            String sintxt2[];
                            sintxt1 = String.valueOf(f).split("index");
                            sintxt2 = sintxt1[1].split(".txt");     //sintxt2 contiene nombre archivo sin extension
                            for (int i = 1; i <= cantidadPartes; i++){
                                machine = Integer.parseInt(bf.readLine());
                                String nombre = "maquina" + machine + sintxt2[0] + i + ".txt";
                                File temp = new File((nombre));
                                if (temp.exists()){                    // COMPRUEBA EXISTENCIA DE PARTES
                                   // System.out.println("PARTE " + String.valueOf(i)+ " de " + temp + " Encontrada "); 
                                }    
                                else{   //NO SE ENCUENTRA ALGUNA PARTE
                                    flag = 0;   //BASTA QUE NO ENCUENTRE UNA PARTE (SEGUN INDEX) PARA NO MOSTRAR
                                }
                            }
                            if (flag == 1){
                                stringBuilder.append(f.getName()+"\n");
                            }
                        }
                        toreturn = stringBuilder.toString();
                        //------------------------------> COMIENZO ESCRIBIR LOG
                        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        Date date = new Date();
                        Charset utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) {
                            writer.write(dateFormat.format(date) + "  ---  COMMAND    ---  " + s.getRemoteSocketAddress().toString() +" --- ls  "+"\n");
                        } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                        //------------------------------> FIN ESCRIBIR LOG
                        dos.writeUTF(toreturn);                 //SE ENVIA INFO A CLIENTE
                        //------------------------------> COMIENZO ESCRIBIR LOG
                        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        date = new Date();
                        utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) {
                            writer.write(dateFormat.format(date) + "  ---  RESPONSE   ---  " + "Servidor envia respuesta a "+ s.getRemoteSocketAddress().toString() +"\n");
                        } catch (IOException e) {System.err.format("IOException: %s%n", e);} 
                        //------------------------------> FIN ESCRIBIR LOG
                        break; 

                    case "delete" :
                        File curDir2 = new File(".");
                        String sintxt [];
                        sintxt = dividir[1].split(".txt");
                        FileReader fr = new FileReader("./index/"+dividir[1]);
                        BufferedReader bf = new BufferedReader(fr);
                        int cantidadPartes = Integer.parseInt(bf.readLine());
                        File archivoIndex = new File("./index/"+dividir[1]);    //ARCHIVO INDEX RELACIONADO
                        int flag = 1;
                        int maquinaN;
                        for(int i = 1; i<= cantidadPartes; i++){    //SE ELIMINAN TODAS LA PARTES DISTRIBUIDAS
                            maquinaN = Integer.parseInt(bf.readLine());
                            String curdir2 = "maquina" + maquinaN + "/" + sintxt[0]+i+".txt";
                            File borrar = new File(curdir2);

                            if(borrar.delete()){
                                dos.writeUTF("El archivo ha sido borrado");     //CORRECTA ELIMINACION DE ARCHIO
                            }
                            else{
                                flag = 0;
                                dos.writeUTF("El archivo no ha sido borrado");  //NO SE ELIMINA ARCHIVO
                            }
                        }
                        bf.close();
                        fr.close();
                        if (flag == 1){
                            archivoIndex.delete();  //SE ELIMINA ARCHIVO DE INDEX
                        }
                       //------------------------------> COMIENZO ESCRIBIR LOG
                        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        date = new Date();
                        utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) {
                            writer.write(dateFormat.format(date) + "  ---  COMMAND    ---  " + s.getRemoteSocketAddress().toString() +" --- delete "+dividir[1]+"\n");
                        } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                        //------------------------------> FIN ESCRIBIR LOG
                        break;
//------------------------------> FUNCION PUT --------------------------------------
                    case "put" : 
                        int bytesRead;
                        String nombre;
                        DataInputStream clientData = new DataInputStream(s.getInputStream());
                        String fileName = clientData.readUTF();
                        long size = clientData.readLong();
                        int aux = (int) size;
                        byte[] buffer = new byte[aux];
                        int numeroArchivos = 1;
                        String nombreArchivo [];
                        nombreArchivo = fileName.split(".txt");
                        int totalMaquinas= Integer.parseInt(cantmaquinasrecibidas);

                        String ruta = "./index/"+fileName;
                        File archivo = new File(ruta);
                        BufferedWriter bw;
                        int maquina = 1; 
                        if(archivo.exists()) {
                            System.out.println("Este Archivo ya existe en Servidor");
                        }
                        else {      //SI NO ESTA ARCHIVO EN SERVIDOR
                                bw = new BufferedWriter(new FileWriter(archivo));
                                bw.write(String.valueOf((size/65500)+1)+"\n");  
                            while (size > 0 && (bytesRead = clientData.read(buffer, 0, (int) Math.min(65500, aux))) != -1) {
                                nombre = "maquina" + maquina + "/" + nombreArchivo[0] + numeroArchivos + ".txt";
                                numeroArchivos += 1;
                                OutputStream output = new FileOutputStream(nombre);
                                output.write(buffer, 0, (int) Math.min(65593, aux));    //ESCRITURA file
                                output.flush();
                                output.close();
                                size -= 65500;
                                aux -= 65500;
                                bw.write(String.valueOf(maquina) + "\n");   //ESCRITURA DE INDEX
                                maquina += 1;
                                if (maquina > totalMaquinas){       //ARCHIVOS SE DISTRIBUYEN DE 1 EN 1 POR CADA MAQUINA
                                    maquina = 1;
                                }
                            }
                            bw.close();
                        }
                        //------------------------------> COMIENZO ESCRIBIR LOG
                        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        date = new Date();
                        utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream("app.log",true), utf8)
                        )) {
                            writer.write(dateFormat.format(date) + "  ---  COMMAND    ---  " + s.getRemoteSocketAddress().toString() +" --- put "+dividir[1]+"\n");
                        } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                        //------------------------------> FIN ESCRIBIR LOG
                        System.out.println("archivo almacenado exitosamente.");

                        //------------------------------> COMIENZO ESCRIBIR LOG
                        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        date = new Date();
                        utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) {
                            writer.write(dateFormat.format(date) + "  ---  RESPONSE   ---  " + "Servidor envia respuesta a "+ s.getRemoteSocketAddress().toString() +"\n");
                        } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                        //------------------------------> FIN DE ESCRIBIR LOG
                        break;
//------------------------------------------- FUNCION GET -----------------------------
                    case "get" :

                        FileReader fr2 = new FileReader("./index/"+dividir[1]);
                        BufferedReader bf2 = new BufferedReader(fr2);
                        int totalArchivos = Integer.parseInt(bf2.readLine());
                        String sacartxt [];
                        String name; 
                        String line;
                        int i2 = 1 ; 
                        int maquina2;
                        File archivo2 = new File(dividir[1]);
                        BufferedWriter bw2 = new BufferedWriter(new FileWriter(archivo2 ,true));
                        PrintWriter pw = new PrintWriter(dividir[1]);
                        for (int i = 1; i <= totalArchivos; i++ ){      //ITERACION POR CADA PARTE
                            maquina2 = Integer.parseInt(bf2.readLine());
                            sacartxt = dividir[1].split(".txt");
                            name = "maquina" + maquina2 + "/" + sacartxt[0] + i +".txt";
                            BufferedReader br = new BufferedReader(new FileReader(name)); 
                            while ((line = br.readLine()) != null) {    //ESCRITURA DE ARCHIVO FINAL EN SERVIDOR
                                bw2.write(line);
                                bw2.newLine();
                            }
                            br.close(); 
                        }
                        bw2.close();
                        //------------------------------> COMIENZO ESCRIBIR LOG
                        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        date = new Date();
                        utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(
                                new OutputStreamWriter(new FileOutputStream("app.log",true), utf8)
                        )) {
                            writer.write(dateFormat.format(date) + "  ---  COMMAND    ---  " + s.getRemoteSocketAddress().toString() +" --- get "+dividir[1]+ "\n");
                        } catch (IOException e) {
                            System.err.format("IOException: %s%n", e);
                        }
                        //------------------------------> FIN ESCRIBIR LOG    

//------------------------- PASAR ARCHIVO A CLIENTE --------------------------------------            
                        File myFile = new File(dividir[1]);
                        byte[] mybytearray = new byte[(int) myFile.length()];
                        FileInputStream fis3 = new FileInputStream(myFile);
                        BufferedInputStream bis3 = new BufferedInputStream(fis3);

                        DataInputStream dis3 = new DataInputStream(bis3);
                        dis3.readFully(mybytearray, 0, mybytearray.length);

                        OutputStream os3 = s.getOutputStream();

                        DataOutputStream dos3 = new DataOutputStream(os3);
                        dos3.writeUTF(myFile.getName());
                        dos3.writeLong(mybytearray.length);
                        dos3.write(mybytearray, 0, mybytearray.length);
                        System.out.println(" Enviado a cliente.");
                        //------------------------------> COMIENZO ESCRIBIR LOG
                        dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                        date = new Date();
                        utf8 = StandardCharsets.UTF_8;
                        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("app.log",true), utf8))) {
                            writer.write(dateFormat.format(date) + "  ---  RESPONSE   ---  " + "Servidor envia respuesta a "+ s.getRemoteSocketAddress().toString() +"\n");
                        } catch (IOException e) {System.err.format("IOException: %s%n", e);}
                        //------------------------------> FIN DE ESCRIBIR LOG
                        dos3.flush();
                        break;

                    default: 
                        dos.writeUTF("Comando no registrado"); 
                        break; 
                } 
            } catch (IOException e) { 
                e.printStackTrace(); 
            } 
        } 
        try
        { 
            this.dis.close(); 
            this.dos.close(); 
              
        }catch(IOException e){ 
            e.printStackTrace(); 
        } 
    } 
} 