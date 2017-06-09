DROP FUNCTION fenamosq.start_session_google(character varying, character varying, character varying, character varying, timestamp without time zone, numeric, character varying);

CREATE OR REPLACE FUNCTION fenamosq.start_session_google(
    IN pi_id_google character varying,
    IN pi_url_img character varying,
    IN pi_id_token character varying,
    IN pi_master character varying,
    IN pi_tran_date timestamp without time zone,
    IN pi_tran_id numeric,
    IN pi_tran_user character varying,
    OUT po_first_name character varying,
    OUT po_last_name character varying,
    OUT po_name character varying,
    OUT po_cod_role character varying,
    OUT po_role_desc character varying,
    OUT po_cod_user character varying,
    OUT po_url_img character varying,
    OUT po_url_img_old character varying,
    OUT po_tran_id numeric,
    OUT po_cod_err character varying,
    OUT po_msj_err character varying,
    OUT po_msj_err_pl character varying)
  RETURNS record AS
$BODY$
  DECLARE
      c_cod_transaction  varchar(200):='START_SESSION_SYS';
      v_tran_id          NUMERIC;
      v_tran_user        VARCHAR(50);
      v_tran_date        TIMESTAMP;      
      v_validar          varchar(20);
      v_pass_in          varchar(2000);
      v_pass_register    varchar(2000);
      c_falso            varchar(20):='0';
      v_aplica           varchar(2):='1';
      c_aplica           varchar(2):='1';
      v_state_person     varchar(6);
      v_state_usr        varchar(6);
      v_url_img          varchar(500);
  BEGIN 
       
      --Obtener valores transacccionales
      IF pi_master = c_aplica THEN 
          
          SELECT rs.po_tran_id,
                 rs.po_tran_date,
                 rs.po_cod_err,
                 rs.po_msj_err,
                 rs.po_msj_err_pl
           INTO  v_tran_id,
                 v_tran_date,
                 po_cod_err,
                 po_msj_err,
                 po_msj_err_pl
           FROM fenamosq.get_transaction(
                pi_cod_transaction    => c_cod_transaction,
                pi_tran_user          => pi_tran_user
           ) rs;

          IF po_cod_err <> 'OK' THEN 
              RAISE EXCEPTION 'TRANSFER';
          END IF;

      ELSIF pi_tran_date IS NULL OR  pi_tran_id IS NULL  OR pi_tran_user  IS NULL THEN
          po_cod_err := 'SESSION-011';
          RAISE EXCEPTION 'CONDICTION';
      ELSE 
          v_tran_id := pi_tran_id;
          v_tran_date  := pi_tran_date;
      END IF; 

      SELECT rs.po_aplica 
        INTO v_aplica 
        FROM fenamosq.validate_transaction(
              pi_cod_transaction => c_cod_transaction,
              pi_tran_user        => v_tran_user
            ) rs;

      IF v_aplica = '0' THEN 
          po_cod_err := 'SESSION-012';
          RAISE EXCEPTION 'CONDICTION';
      ELSIF v_aplica = '2' THEN 
          po_cod_err := 'SESSION-003';
          RAISE EXCEPTION 'CONDICTION';
      END IF;


    po_cod_err := 'SESSION-013';
    SELECT initcap(per.first_name),
           initcap(per.last_name),
           initcap(per.first_name||' '||per.last_name),
           per.url_img,
           per.state,
           usr.state,
           usr.cod_user,
           rol.cod_role,
           per.url_img
      INTO po_first_name,
           po_last_name,
           po_name,
           v_url_img,
           v_state_person,
           v_state_usr,
           po_cod_user,
           po_cod_role,
           po_url_img
      FROM fenamosq.emp_user usr, fenamosq.emp_person per, fenamosq.emp_role_user rol
     WHERE per.id_person = usr.id_person
       AND per.id_google = pi_id_google
       AND rol.cod_user = usr.cod_user
       AND rol.state = 'A'
       AND rol.tran_date_end IS NULL;



    SELECT initcap(description) description
      INTO po_role_desc
      FROM fenamosq.emp_role
     WHERE cod_role = po_cod_role;

     IF po_first_name IS NULL THEN 
         po_cod_err := 'SESSION-014';
         RAISE EXCEPTION 'CONDICTION';
     ELSIF v_state_usr <> 'A' OR v_state_person <> 'A' THEN
         po_cod_err := 'SESSION-05';
         RAISE EXCEPTION 'CONDICTION';
     END IF; 
    
     IF v_url_img IS NULL OR v_url_img <> pi_url_img THEN
        UPDATE fenamosq.emp_person
           SET url_img = pi_url_img
         WHERE id_google = pi_id_google;
     END IF;
     po_url_img_old = po_url_img;
     po_url_img = pi_url_img;
     po_cod_err := 'OK';
     po_tran_id := v_tran_id;


  EXCEPTION
    WHEN OTHERS THEN 
        po_tran_id := v_tran_id;

        IF(SQLERRM <> 'TRANSFER') THEN 
            po_msj_err_pl = SQLERRM;
        END IF;

        IF pi_master = c_aplica THEN
          
          SELECT rs.po_msj_err
            into po_msj_err 
            FROM fenamosq.set_transaction_log(
                pi_tran_id         => v_tran_id,
                pi_tran_cod        => c_cod_transaction,
                pi_tran_date       => v_tran_date,    
                pi_tran_user       => pi_tran_user,
                pi_cod_err         => po_cod_err,
                pi_msj_err_pl      => po_msj_err_pl) rs;
        END IF;


  END; 
  $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION fenamosq.start_session_google(character varying, character varying, character varying, character varying, timestamp without time zone, numeric, character varying)
  OWNER TO postgres;
